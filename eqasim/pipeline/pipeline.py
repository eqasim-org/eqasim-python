import logging
import os, sys
import pickle
import uuid
import json, yaml
import importlib
import shutil
import multiprocessing as mp
from . import progress

class PipelineException(Exception):
    pass

def _is_config_value(value):
    for type in (str, int, float, bool):
        if isinstance(value, type):
            return True

    return False

def _flatten_config(config):
    queue = [([], config)]
    flat_config = {}

    while len(queue) > 0:
        path, item = queue.pop(0)

        for key, value in item.items():
            full_key = ".".join(path + [key])

            if "." in key:
                raise PipelineException("Config keys should not contain dots: %s" % full_key)

            if _is_config_value(value):
                flat_config[full_key] = value
            elif isinstance(value, dict):
                queue.append(([path] + [key], value))
            else:
                raise PipelineException("Illegal data type '%s' for config key %s" % (type(value), full_key))

    return flat_config

def _flatten_dag(dependencies):
    sequence = []
    remaining = list(dependencies.keys())

    while len(remaining) > 0:
        prior_count = len(remaining)

        for item in remaining[:]:
            insertable = True

            for dependency in dependencies[item]:
                insertable = insertable and (dependency in sequence)

            if insertable:
                sequence.append(item)
                remaining.remove(item)

        posterior_count = len(remaining)

        if prior_count == posterior_count:
            raise PipelineException("Found circular dependency: " + str(remaining))

    return sequence

def _filter_config(config, keys):
    filtered_config = {}

    for key in keys:
        filtered_config[key] = config[key]

    return filtered_config

class Require:
    def __init__(self):
        self._config = {}
        self._stages = set()

    def config(self, key, default = None):
        self._config[key] = default

    def stage(self, name):
        self._stages.add(name)

class ConfigContext:
    def __init__(self, _config):
        self._config = _config

    def config(self, key):
        if not key in self._config:
            raise PipelineException("Config key does not exist (or is not requested): %s" % key)

        return self._config[key]

class ExecutorConfig(ConfigContext):
    def __init__(self, _name, _parent_stages, _config, _working_directory, _progress_port):
        super().__init__(_config)

        self._working_directory = _working_directory
        self._name = _name
        self._parent_stages = _parent_stages
        self._progress_port = _progress_port

    def stage(self, name):
        if not name in self._parent_stages:
            raise PipelineException("Stage %s is not requested" % name)

        with open("%s/%s_result.p" % (self._working_directory, name), "rb") as f:
            return pickle.load(f)

    def cache_path(self, name = None):
        if name is None:
            return "%s/%s_cache" % (self._working_directory, self._name)
        else:
            if not name in self._parent_stages:
                raise PipelineException("Stage %s is not requested" % name)

            return "%s/%s_cache" % (self._working_directory, name)

    def progress(self, desc = None, total = None, interval = None):
        return progress.PipelineProgress(port = self._progress_port, total = total, desc = desc, interval = interval)

def run(config):
    # We need a minimum amount of information for the pipeline
    if not "working_directory" in config:
        raise PipelineException("No working_directory specified in config")

    if not "stages" in config:
        raise PipelineException("No stages specified in config")

    working_directory = config["working_directory"]
    requested_stages = set(config["stages"])

    del config["working_directory"]
    del config["stages"]

    # Flatten the configuration
    config = _flatten_config(config)

    # Gather information for all relevant stages (through "configure")
    logging.info("Gathering informaton for all relevant stages ...")

    pending_stages = list(requested_stages)
    available_stages = {}

    stage_dependencies = {}
    config_dependencies = {}

    while len(pending_stages) > 0:
        pending_stage = pending_stages.pop(0)

        module = importlib.import_module(pending_stage)
        available_stages[pending_stage] = module

        require = Require()

        if "configure" in module.__dict__:
            module.configure(require)

        config_dependencies[pending_stage] = require._config
        stage_dependencies[pending_stage] = require._stages

        for dependency in require._stages:
            if not dependency in available_stages and not dependency in pending_stages:
                pending_stages.append(dependency)

    # Check consistency of configuration requirements
    logging.info("Checking configuration requirements ...")
    missing_keys = set()

    for stage, dependencies in config_dependencies.items():
        for key in dependencies.keys():
            if not key in config:
                logging.error("Stage %s requires unknown config key: %s" % (stage, key))
                missing_keys.add(key)

    if len(missing_keys) > 0:
        raise PipelineException("Missing config keys: " + str(missing_keys))

    # Check consistency of default values
    default_values = { k : {} for k in config.keys() }

    for stage, dependencies in config_dependencies.items():
        for key, default_value in dependencies.items():
            if not default_value is None:
                key_default_values = default_values[key]

                if not default_value in key_default_values:
                    key_default_values[default_value] = []

                key_default_values[default_value].append(stage)

    consistent_default_values = True

    for key, key_default_values in default_values.items():
        if len(key_default_values) > 1:
            logging.error("Multiple default values for key %s:" % key)
            consistent_default_values = False

            for default_value, stages in key_default_values.items():
                logging.error("  %s (from %s)" % (default_value, ", ".join(stages)))

    if not consistent_default_values:
        raise PipelineException("Default config values are not consistent (see log)")

    # Apply default values to configuration
    for key, key_default_values in default_values.items():
        if not key in config:
            config[key] = list(key_default_values.keys())[0]

    # Check presence of all required config values
    config_present = True

    for stage, dependencies in config_dependencies.items():
        for key in dependencies.keys():
            if not key in config:
                logging.error("Missing config key %s (for stage %s)" % (key, stage))
                config_present = False

    if not config_present:
        raise PipelineException("Some config keys are missing (see log)")

    # Construct DAG
    sequence = _flatten_dag(stage_dependencies)

    # Determine the set of stale stages
    logging.info("Checking for stale stages ...")

    stale_stages = set(requested_stages)
    verification_tokens = {}

    expected_uuids = { stage: {} for stage in stage_dependencies.keys() }
    current_uuids = { stage: None for stage in stage_dependencies.keys() }

    for stage in sequence:
        # Obtian current verification token
        module = available_stages[stage]
        verification_token = None

        if "verify" in module.__dict__:
            verification_token = module.verify(ConfigContext(_filter_config(
                config, config_dependencies[stage].keys()
            )))

        verification_tokens[stage] = verification_token
        is_requested = stage in requested_stages

        # First, check if configuraton is valid
        config_valid = False
        cached_config = None

        cached_config_path = "%s/%s_config.yml" % (working_directory, stage)

        if os.path.isfile(cached_config_path):
            config_valid = True

            with open(cached_config_path) as f:
                cached_config = json.load(f)

                for key in ("__uuid", "__expected_uuids", "__verification_token"):
                    if not key in cached_config:
                        config_valid = False

        if not config_valid:
            if not is_requested:
                logging.info("  Verification failed for %s (invalid or doesn't exist)" % stage)
                stale_stages.add(stage)

        # Check if cache and result is available

        if not os.path.isfile("%s/%s_result.p" % (working_directory, stage)) and not is_requested:
            logging.info("  Verification failed for %s (no result)" % stage)
            stale_stages.add(stage)

        if not os.path.isdir("%s/%s_cache" % (working_directory, stage)) and not is_requested:
            logging.info("  Verification failed for %s (no cache)" % stage)
            stale_stages.add(stage)

        # Second, check verification token
        if config_valid:
            expected_uuids[stage] = cached_config["__expected_uuids"]
            current_uuids[stage] = cached_config["__uuid"]
            cached_verification_token = cached_config["__verification_token"]

            if not verification_token == cached_verification_token and not is_requested:
                logging.info("  Verification failed for %s (stale token)" % stage)
                stale_stages.add(stage)

        # Third, check if configuratio nis updated
        if config_valid:
            updated_keys = set()

            for key in config_dependencies[stage]:
                if not key in cached_config or not cached_config[key] == config[key]:
                    updated_keys.add(key)

            if len(updated_keys) > 0:
                logging.info("  Configuration updated for %s (%s)" % (stage, updated_keys))
                stale_stages.add(stage)

    # Find all required stages
    for stage in sequence:
        if not stage in stale_stages:
            # Mark stages as stale that have stale or requested parent
            stale_parents = stage_dependencies[stage] & stale_stages

            if len(stale_parents) > 0:
                logging.info("  Marking %s as stale (because %s is stale/requested)" % (stage, stale_parents))
                stale_stages.add(stage)

        if not stage in stale_stages:
            # Mark stages as stale that expect different UUID in parent
            for parent_stage, expected_parent_uuid in expected_uuids[stage].items():
                if not expected_parent_uuid == current_uuids[parent_stage]:
                    stale_stages.add(stage)
                    logging.info("  Marking %s as stale (because %s has changed)" % (stage, parent_stage))
                    break

    # Execute stages
    logging.info("Will run the following stages: %s" % [stage for stage in sequence if stage in stale_stages])

    progress_port = progress.get_random_port()
    progress_process = mp.Process(target = progress.run_server, args = (progress_port,))
    progress_process.start()

    for stage in sequence:
        if stage in stale_stages:
            logging.info("Executing %s ..." % stage)
            module = available_stages[stage]

            if not "execute" in module.__dict__:
                raise PipelineException("No executor for stage %s" % stage)

            cache_path = "%s/%s_cache" % (working_directory, stage)

            if os.path.isdir(cache_path):
                shutil.rmtree(cache_path)

            os.mkdir(cache_path)

            stage_config = _filter_config(config, config_dependencies[stage])
            result = module.execute(ExecutorConfig(stage, stage_dependencies[stage], stage_config, working_directory, progress_port))

            with open("%s/%s_result.p" % (working_directory, stage), "wb+") as f:
                pickle.dump(result, f)

            updated_uuid = str(uuid.uuid1())

            stage_config["__uuid"] = updated_uuid
            current_uuids[stage] = updated_uuid

            stage_config["__expected_uuids"] = { parent: current_uuids[parent] for parent in stage_dependencies[stage] }
            stage_config["__verification_token"] = verification_tokens[stage]

            with open("%s/%s_config.yml" % (working_directory, stage), "w+") as f:
                json.dump(stage_config, f)

    # Finish line
    logging.info("All stages have been executed.")

    progress_client = progress.ProgressClient(port = progress_port)
    progress_client.close()
    progress_process.join()
