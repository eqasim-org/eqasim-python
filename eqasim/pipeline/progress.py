import zmq
import multiprocessing as mp
import logging
import socket as sck
import time
import uuid as uuid_generator

class ProgressServer:
    def __init__(self, port):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind("tcp://*:%d" % port)

        self.closed = False
        logging.debug("Starting progress server")

        self.running = {}

    def run(self):
        while not self.closed:
            message = self.socket.recv_json()
            self.socket.send_json({})

            if message["command"] == "close":
                self.close()
            elif message["command"] == "initialize":
                self.initialize(message["uuid"], message["total"], message["desc"], message["interval"])
            elif message["command"] == "finalize":
                self.finalize(message["uuid"])
            elif message["command"] == "update":
                self.update(message["uuid"], message["count"])

    def close(self):
        logging.debug("Closing progress server")
        self.closed = True

    def initialize(self, uuid, total, desc, interval):
        logging.debug("Initialize %s" % uuid)

        if interval is None:
            interval = 1.0

        self.running[uuid] = {
            "total": total, "desc": desc, "current": 0, "last_print": 0, "interval": interval, "start_time": time.time()
        }

    def finalize(self, uuid):
        logging.debug("Finalize %s" % uuid)
        if uuid in self.running:
            self.running[uuid]["last_print"] = 0
            self.print(uuid)

            del self.running[uuid]

    def update(self, uuid, count):
        logging.debug("Update %s" % uuid)

        if count is None:
            count = 1

        if uuid in self.running:
            item = self.running[uuid]
            item["current"] += count

            if time.time() - item["last_print"] > item["interval"]:
                item["last_print"] = time.time()
                self.print(uuid)

    def print(self, uuid):
        if uuid in self.running:
            item = self.running[uuid]

            message = ["Progress" if item["desc"] is None else item["desc"]]

            if item["total"] is None:
                message.append("[%d]" % item["current"])
            else:
                progress = item["current"] / item["total"]

                current_str = str(item["current"])
                total_str = str(item["total"])
                current_str = " " * (len(total_str) - len(current_str)) + current_str

                message.append("%s/%d (% 7.2f%%)" % (
                    current_str, item["total"], 100.0 * progress
                ))

                ticks = round(progress * 10)
                message.append("[%s%s]" % ("#" * ticks, " " * (10 - ticks)))

            runtime = time.time() - item["start_time"]
            speed = item["current"] / runtime

            if speed >= 1.0:
                message.append("%.2f it/s" % speed)
            else:
                message.append("%.2f s/it" % (1.0 / speed))

            print(" ".join(message))

class ProgressClient:
    def __init__(self, port):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect("tcp://localhost:%d" % port)
        self.port = port

    def initialize(self, uuid, total = None, desc = None, interval = None):
        self.socket.send_json({ "command": "initialize", "uuid": uuid, "total": total, "desc": desc, "interval": interval })
        self.socket.recv_json()

    def finalize(self, uuid):
        self.socket.send_json({ "command": "finalize", "uuid": uuid })
        self.socket.recv_json()

    def update(self, uuid, count = None):
        self.socket.send_json({ "command": "update", "uuid": uuid, "count": count })
        self.socket.recv_json()

    def close(self):
        self.socket.send_json({ "command": "close" })
        self.socket.recv_json()

def get_random_port():
    socket = sck.socket(sck.AF_INET, sck.SOCK_STREAM)
    socket.bind(("localhost", 0))
    socket.listen(1)

    port = socket.getsockname()[1]
    socket.close()

    return port

def run_server(port):
    server = ProgressServer(port)
    server.run()

class PipelineProgress:
    def __init__(self, client = None, uuid = None, port = None, desc = None, total = None, interval = None, initialize = True):
        self.client = client
        self.port = port
        self.uuid = uuid

        if uuid is None:
            self.uuid = str(uuid_generator.uuid1())
        else:
            self.uuid = uuid

        if initialize:
            self._client().initialize(self.uuid, total, desc, interval)

    def _client(self):
        if self.client is None:
            if self.port is None:
                raise RuntimeException("Neither port nor client are available")

            self.client = ProgressClient(self.port)

        return self.client

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._client().finalize(self.uuid)

    def update(self, count = None):
        self._client().update(self.uuid, count)

    def parallel(self):
        return PipelineProgress(port = self.port, uuid = self.uuid, initialize = False)

def run_client(progress):
    for i in range(100):
        progress.update(1)
        time.sleep(0.5)

if __name__ == "__main__":
    logging.basicConfig(level = logging.INFO)

    port = get_random_port()

    process = mp.Process(target = run_server, args = (port,))
    process.start()

    with PipelineProgress(port = port, desc = "Whatever") as progress:
        processes = 5
        with mp.Pool(processes) as pool:
            handlers = [pool.apply_async(run_client, args = (progress.parallel(),)) for i in range(processes)]
            results = [h.get() for h in handlers]

    main_client = ProgressClient(port)
    main_client.close()

    process.join()
