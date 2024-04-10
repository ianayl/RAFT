import time
import threading
import grpc
from google.protobuf.empty_pb2 import Empty
from concurrent import futures
import heartbeat_service_pb2
import heartbeat_service_pb2_grpc

# Seconds per heartbeat
HEARTBEAT_RATE = 5
# Number of heartbeats until a service is considered down
HEARTBEATS_UNTIL_DOWN = 2
# Check for dead services every second
HEARTBEAT_POLL_RATE = 1


# Servicer stays separate from heartbeat server itself
class ViewServiceServicer(heartbeat_service_pb2_grpc.ViewServiceServicer):
    def __init__(self, update_heartbeat_fn):
        self.update_heartbeat_fn = update_heartbeat_fn

    def Heartbeat(self, req, ctx):
        self.update_heartbeat_fn(req.service_identifier)
        return Empty()


class HeartbeatServer:

    def __init__(self, port, log_file="heartbeat.txt"):
        self.port = port
        self.latest_heartbeat = dict()
        self.log_file = log_file
        with open(log_file, 'w') as _: print(f"Warning: clearing prior logs in {self.log_file}")  
        self.lock = threading.Lock()
        self._running = False

    def update_heartbeat(self, identifier: str):
        timestamp = time.time()
        with self.lock:
            self.latest_heartbeat[identifier] = timestamp
            with open(self.log_file, 'a') as f:
                f.write(f"{identifier} is alive. Latest heartbeat received at {timestamp}\n")  
        print(f"Heartbeat: Received heartbeat from {identifier} at {timestamp}.")

    def poll_heartbeats(self):
        while self._running:
            time.sleep(HEARTBEAT_POLL_RATE)
            timestamp = time.time()
            dead_services = []
            with self.lock:
                for identifier in self.latest_heartbeat:
                    if timestamp > (self.latest_heartbeat[identifier] + 
                                    HEARTBEATS_UNTIL_DOWN * HEARTBEAT_RATE):
                        with open(self.log_file, 'a') as f:
                            f.write(f"{identifier} might be down. Latest heartbeat "
                                    f"received at {self.latest_heartbeat[identifier]}\n")  
                        print(f"Heartbeat: No heartbeat from {identifier} since {timestamp},"
                              f" assuming {identifier} is dead!")
                        dead_services.append(identifier)
                # Stop tracking dead services
                for dead in dead_services: del self.latest_heartbeat[dead]


    def start(self):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        self.rpc_servicer = ViewServiceServicer(self.update_heartbeat)
        heartbeat_service_pb2_grpc.add_ViewServiceServicer_to_server(self.rpc_servicer,
                                                                     self.server)
        self.server.add_insecure_port(f'[::]:{self.port}')

        self._running = True
        heartbeat_poll_thread = threading.Thread(target=self.poll_heartbeats)
        heartbeat_poll_thread.start()
        self.server.start()
        self.server.wait_for_termination()
        self._running = False
        heartbeat_ping_thread.wait_for_termination()


if __name__ == "__main__":
    heart = HeartbeatServer(50053)
    heart.start()
