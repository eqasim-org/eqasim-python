from . import pipeline
import logging
import os
import yaml

def _adapt_config(config, key, value):
    # Try to convert to numeric data types
    try:
        value = int(value)
    except ValueError:
        try:
            value = float(value)
        except ValueError:
            pass

    # Set in config tree
    current = config
    segments = key.split(".")

    for i in range(len(segments)):
        if i == len(segments) - 1:
            current[segments[i]] = value
        else:
            if not segments[i] in current:
                current[segments[i]] = {}

            current = current[segments[i]]

if __name__ == "__main__":
    import sys

    logging.basicConfig(level = logging.INFO)

    if len(sys.argv) < 2:
        raise pipeline.PipelineException("Path to config required as first argument")

    if not os.path.isfile(sys.argv[1]):
        raise pipeline.PipelineException("Path does not exist: %s" % sys.argv[1])

    with open(sys.argv[1]) as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

        if not "stages" in config:
            config["stages"] = []

        for index in range(2, len(sys.argv) - 1):
            key = sys.argv[index]
            value = sys.argv[index + 1]

            if key.startswith("--set:"):
                _adapt_config(config, key[6:], value)
                index += 1
            elif key == "--stages":
                stages = list(set([stage.strip() for stage in value.split(",")]))
                index += 1

                for stage in stages:
                    if stage[0] == "+":
                        config["stages"] = list(set(config["stages"]) | set([stage[1:]]))
                    elif stage[0] == "-":
                        config["stages"] = list(set(config["stages"]) - set([stage[1:]]))
                    else:
                        raise PipelineException("Stages via command line must start with +/-")
            else:
                logging.info("Ignoring command line argument: %s" % key)

        pipeline.run(config)
