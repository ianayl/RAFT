import grpc
import threading
import time
from concurrent import futures
import replication_pb2
import replication_pb2_grpc
# import heartbeat_service_pb2
# import heartbeat_service_pb2_grpc

from constants import KNOWN_REPLICAS

# Seconds per heartbeat
# HEARTBEAT_RATE = 5

NAK="NAK"
ACK="ACK"

class SequenceServicer(replication_pb2_grpc.SequenceServicer):
    def __init__(self, port, identifier: int, replicas=[]):
        self.store = dict()
        self.port = port
        self.replicas = replicas
        # TODO these identifiers + log files WONT FLY for multiple backups!
        self.identifier = identifier
        self.log_file = "primary.txt" if len(replicas) > 0 else f"{self.identifier}.txt"
        # Wipe my logfile from prior input
        with open(self.log_file, 'w') as _: print(f"Warning: clearing prior logs in {self.log_file}")  
        # host:port can be used to distinguish between different backups

        # Persistent states
        self.currentTerm = 0
        self.votedFor = None
        self.log = []

        # Volatile states
        self.commitIndex = 0
        self.lastApplied = 0

        # Volatile leader states
        self.nextIndex = []
        self.matchIndex = []

        # Election timeout
        self.timeout = 10
    
    def Write(self, req, ctx):
        # Try to write to every backup:
        for b in self.replicas:
            with grpc.insecure_channel(b) as channel:
                print(f"{self.identity}: Writing to {b}...")
                backup_server = replication_pb2_grpc.SequenceStub(channel)
                try:
                    res = backup_server.Write(req)
                    if res.ack == NAK:
                        print(f"Received NAK from {b}! NAK'ing...")
                        return res
                except grpc.RpcError as e:
                    # If backup is offline, that's okay, just NAK
                    if e.code() == grpc.StatusCode.UNAVAILABLE:
                        print(f"Unable to reach {b}! NAK'ing...")
                        return replication_pb2.WriteResponse(ack=NAK)
                    # Otherwise I don't know the error, so crash:
                    else: raise e

        # Every backup has ACK'd a write, write can proceed
        self.store[req.key] = req.value
        with open(self.log_file, 'a') as f:
            f.write(f"{req.key} {req.value}\n")
        print(f"{self.identity}: Wrote ({req.key}: {req.value}).")
        return replication_pb2.WriteResponse(ack=ACK)

    def AppendEntries(self, req, ctx):
        # req.term

    def RequestVote(self, request, context):
        # If request is from an older term, reject it
        if request.term < self.currentTerm:
            return replication_pb2.RequestVoteResponse(term=self.currentTerm, vote_granted=False)
        
        # If I haven't voted yet, vote for the candidate
        if (self.votedFor is None or self.votedFor == request.candidate_id) and \
            (request.last_log_index >= self.lastApplied):
            self.votedFor = request.candidate_id
            return replication_pb2.RequestVoteResponse(term=self.currentTerm, vote_granted=True)

class Replica():
    """Base class for all replicas"""
    def __init__(self, port: int, identifier: int, other_replicas=KNOWN_REPLICAS):
        """
        Create a replica class -- pass in a list of backups into primary_backups
        to create a primary replica. Otherwise, replicas are created as backups
        by default.

        @param port             port number to run on
        @param other_replicas   list of other replicas
        @param primary          is our replica a primary?
        """
        self.port = port
        self.other_replicas = other_replicas
        self.identifier = identifier
        self._running = False

    # def ping_heartbeat(self):
    #     while self._running:
    #         with grpc.insecure_channel(f"localhost:{self.heartbeat_port}") as channel:
    #             heartbeat_stub = heartbeat_service_pb2_grpc.ViewServiceStub(channel)
    #             try:
    #                 res = heartbeat_stub.Heartbeat(heartbeat_service_pb2.HeartbeatRequest(service_identifier=self.identity))
    #             except grpc.RpcError as e:
    #                 # If heartbeat server is dead, simply ignore
    #                 if e.code() != grpc.StatusCode.UNAVAILABLE:
    #                     raise e
    #                 print("Warning: unable to reach heartbeat server")
    #         time.sleep(HEARTBEAT_RATE)

    def start(self):
        """Start the replica server"""
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        self.rpc_servicer = SequenceServicer(self.port, self.identifier, replicas=self.other_replicas)
        replication_pb2_grpc.add_SequenceServicer_to_server(self.rpc_servicer, self.server)
        self.server.add_insecure_port(f'[::]:{self.port}')

        self._running = True
        # TODO periodically ping primary instead!
        # if self.heartbeat_port is not None:
        #     heartbeat_ping_thread = threading.Thread(target=self.ping_heartbeat)
        #     heartbeat_ping_thread.start()
        self.server.start()
        self.server.wait_for_termination()
        self._running = False
       #  if self.heartbeat_port is not None:
       #      heartbeat_ping_thread.wait_for_termination()
