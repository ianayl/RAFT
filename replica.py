import grpc
import threading
import time
from concurrent import futures
import replication_pb2
import replication_pb2_grpc
import raft_pb2
import raft_pb2_grpc
import random
# import heartbeat_service_pb2
# import heartbeat_service_pb2_grpc

from constants import KNOWN_REPLICAS

# Seconds per heartbeat
# HEARTBEAT_RATE = 5

NAK="NAK"
ACK="ACK"

class SequenceServicer(replication_pb2_grpc.SequenceServicer):
    def __init__(self, identifier: int, leader: int, replicas: list):
        self.store = dict()
        self.replicas = replicas
        self.identifier = identifier
        self.leader = leader
        self.log_file = f"log_{self.identifier}.txt"
        # Wipe my logfile from prior input
        with open(self.log_file, 'w') as _: print(f"Warning: clearing prior logs in {self.log_file}")  

        # Persistent states
        self.currentTerm = 0
        self.votedFor = None
        self.log = dict()          # pair of (log index, log entry)
        # I assume here that the dictionaries are ordered!

        # Volatile states
        self.commitIndex = 0
        self.lastApplied = 0

        # Volatile leader states
        self.nextIndex  = { rep: self.commitIndex + 1 for rep in replicas }  # pair of (server index, next log index)
        self.matchIndex = { rep: 0                    for rep in replicas }  # pair of (server index, highest log entry replicated)

        self.lastHeartbeat = time.time_ns()
    
    # def new_leader(self, leader_id: int):
    #     self.leader = leader_id

    def Write(self, req, ctx):
        # If I am not the leader, redirect:
        if self.identifier != self.leader:
            with grpc.insecure_channel(f"localhost:{self.leader}") as channel:
                leader_stub = replication_pb2_grpc.SequenceStub(channel)
                print(f"{self.identifier}: Not leader, redirecting to leader (f{self.leader})...")
                try:
                    res = leader_stub.Write(req)
                    if res.ack == NAK:
                        print(f"Received NAK from {self.leader}! NAK'ing...")
                        return res
                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.UNAVAILABLE:
                        # TODO leader offline, trigger election instead
                        print(f"Unable to reach {self.leader}! NAK'ing...")
                        return replication_pb2.WriteResponse(ack=NAK)
                    # Otherwise I don't know the error, so crash:
                    else: raise e
            return replication_pb2.WriteResponse(ack=ACK)

        # I am the leader: append to log, and ask others to append:
        self.log[self.commitIndex + 1] = raft_pb2.LogEntry(
            index  = self.commitIndex + 1,
            term   = self.currentTerm,
            opcode = "set",
            key    = req.key,
            val    = req.value
        )

        for rep_id in [ rep for rep in self.replicas if rep != self.identifier ]:
            while True:
                print(self.nextIndex)
                new_entries = [ entry for entry in self.log.values() if entry.index >= self.nextIndex[rep_id] ]
                print(type(new_entries[0]))
                print(type(self.log[1]))
                if not new_entries: break

                with grpc.insecure_channel(f"localhost:{rep_id}") as channel:
                    print(f"{self.identifier} (leader): Writing to {rep_id}...")
                    replica_stub = replication_pb2_grpc.SequenceStub(channel)
                    try:
                        not_stupid = raft_pb2.AppendEntriesRequest(
                                    term=self.currentTerm,
                                    leader_id=f"{self.identifier}",
                                    prev_log_index=self.nextIndex[rep_id],
                                    prev_log_term=self.log[self.nextIndex[rep_id]].term,
                                    entries=new_entries,
                                    leader_commit=self.commitIndex
                                )
                        res = replica_stub.AppendEntries(not_stupid)
                        if res.success:
                            # TODO this is sussy
                            self.nextIndex[rep_id]  = self.commitIndex + 2
                            self.matchIndex[rep_id] = self.commitIndex + 1
                            break
                        else:
                            self.nextIndex[rep_id] -= 1
                    except grpc.RpcError as e:
                        # If replica is offline, I can just skip
                        # TODO is this true ^^^
                        if e.code() == grpc.StatusCode.UNAVAILABLE:
                            print(f"Unable to reach {rep_id}! Skipping...")
                        # Otherwise I don't know the error, so crash:
                        else:
                            print("ERROR:")
                            print(e)
                        break
        # If there exists an N such that N > commitIndex, a majority of
        # matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
        potential_N = [ n for n in self.matchIndex.values()
                          if n > self.commitIndex and
                             sum([ 1 if match_i >= n else 0 for match_i in self.matchIndex.values() ]) >= (len(self.matchIndex) / 2) and
                             self.log[n].term == self.currentTerm ]
        if potential_N:
            for i in range(self.commitIndex, max(potential_N) + 1):
                if i in self.log:
                    print(f"{self.log[i].opcode} {self.log[i].key} {self.log[i].val}")
            self.commitIndex = max(potential_N)
        # max_n, max_majority = -1, -1
        # for n in potential_N:
        #     majority = sum([ 1 if match_i >= n else 0 for match_i in self.matchIndex.values() ])
        #     if majority >= (len(self.matchIndex) / 2):
        #         max_n, max_majority = n, majority
        # if max_n != -1:
        #     for ()
        #         print(f"{entry.opcode} {entry.key} {entry.val}")
        #     commitIndex = max_n

        return replication_pb2.WriteResponse(ack=ACK)
            

    def _delete_after_index(self, index: int):
        for i in self.log:
            if i >= index: del self.log[i]

    def AppendEntries(self, req, ctx):
        # I received something from a leader, acknowledge that leader instead
        self.lastHeartbeat = time.time_ns()
        self.leader = int(req.leader_id)

        # Reply false if term < currentTerm
        if req.term < self.currentTerm:
            return raft_pb2.AppendEntriesResponse(term=self.currentTerm, success=False)

        # Reply false if log doesn’t contain an entry at prevLogIndex whose term matches
        # prevLogTerm
        if self.log and self.log[req.prev_log_index].term != req.prev_log_term:
            return raft_pb2.AppendEntriesResponse(term=self.currentTerm, success=False)

        # If an existing entry conflicts with a new one (same index but different terms),
        # delete the existing entry and all that follow it
        for entry in req.entries:
            if entry.index in self.log and entry.term != self.log[entry.index].term:
                self._delete_after_index(entry.index)

        # Append any new entries not already in the log
        for entry in req.entries:
            if entry.index not in self.log:
                self.log[entry.index] = entry
                # TODO add to db here
                print(f"{entry.opcode} {entry.key} {entry.val}")

        # If leaderCommit > commitIndex, set:
        #     commitIndex = min(leaderCommit, index of last new entry)
        if req.leader_commit > self.commitIndex:
            self.commitIndex = min(req.leader_commit, req.entries[-1].index)

        return raft_pb2.AppendEntriesResponse(term=self.currentTerm, success=True)

    def RequestVote(self, request, context):
        # Reset election timeout on vote request
        self.lastHeartbeat = time.time_ns()

        # If request is from an older term, reject it
        if request.term < self.currentTerm:
            return raft_pb2.RequestVoteResponse(term=self.currentTerm, vote_granted=False)
        
        # If I haven't voted yet, vote for the candidate
        if (self.votedFor is None or self.votedFor == request.candidate_id) and \
            (request.last_log_index >= self.lastApplied):
            self.votedFor = request.candidate_id
            return raft_pb2.RequestVoteResponse(term=self.currentTerm, vote_granted=True)

class Replica():
    """Base class for all replicas"""
    def __init__(self, port: int, other_replicas=KNOWN_REPLICAS, primary=False):
        """
        Create a replica class -- pass in a list of backups into primary_backups
        to create a primary replica. Otherwise, replicas are created as backups
        by default.

        @param port             port number to run on
        @param other_replicas   list of other replicas
        @param primary          is our replica a primary?
        """
        self.other_replicas = other_replicas
        self.identifier = port    # port is identifier
        self.state = "primary" if primary else "backup"
        self._running = False
        self.leader = self.identifier if self.state == "primary" else None

        # Randomize election timeout
        random.seed(time.time_ns())

        # Election timeout
        self.timeout = random.randrange(150, 300) * 1000000 # 150-300ms converted to ns
        self.heartbeat_rate = 50000000 # 50ms converted to ns

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
    
    def start_election(self, server):
        # Increment term and vote for self
        server.currentTerm += 1
        server.votedFor = server.identifier
        votes = 1
        numReplicas = len(server.replicas)

        # Request votes from all other servers
        for replica in server.replicas:
            with grpc.insecure_channel(f"localhost:{replica}") as channel:
                replica_server = replication_pb2_grpc.SequenceStub(channel)
                try:
                    res = replica_server.RequestVote(raft_pb2.RequestVoteRequest(
                        term=server.currentTerm,
                        candidate_id=str(server.identifier),
                        last_log_index=server.lastApplied
                    ))
                    if res.vote_granted:
                        votes += 1
                except grpc.RpcError as e:
                    print(f"Error: {e.code()} - {e.details()}")
                    numReplicas -= 1
        
        # If votes > n/2, become primary
        if votes > numReplicas // 2:
            server.leader = server.identifier
            # Send empty AppendEntries RPCs to all other servers
            for replica in server.replicas:
                with grpc.insecure_channel(f"localhost:{replica}") as channel:
                    replica_server = replication_pb2_grpc.SequenceStub(channel)
                    try:
                        res = replica_server.AppendEntries(raft_pb2.AppendEntriesRequest(
                            term=server.currentTerm,
                            leader_id=str(server.identifier),
                            prev_log_index=server.lastApplied,
                            prev_log_term=server.currentTerm,
                            entries=[],
                            leader_commit=server.commitIndex
                        ))
                    except grpc.RpcError as e:
                        print(f"Error: {e.code()} - {e.details()}")

            # Reset leader states
            server.nextIndex = { replica: server.lastApplied + 1 for replica in server.replicas }
            server.matchIndex = { replica: 0 for replica in server.replicas }
            print(f"{server.identity}: Elected primary for term {server.currentTerm}.")


    def election_timeout(self, server):
        """Election timeout for Raft"""
        while self._running:
            # Ignore election timeout if we're the primary
            if server.leader == server.identifier: continue
            if time.time_ns() - server.lastHeartbeat > self.timeout:
                server.lastHeartbeat = time.time_ns()
                print(f"Primary timed out. Starting election.")
                self.start_election(server)
            # If we're a backup, we need to start an election

    def primary_heartbeat(self, server):
        """heartbeat"""
        while self._running:
            # Only run if leader
            if server.leader != server.identifier: continue 

            if time.time_ns() - server.lastHeartbeat >= self.heartbeat_rate:
                server.lastHeartbeat = time.time_ns()
                for replica in server.replicas:
                    if replica == server.identifier: continue
                    with grpc.insecure_channel(f"localhost:{replica}") as channel:
                        replica_server = replication_pb2_grpc.SequenceStub(channel)
                        try:
                            res = replica_server.AppendEntries(raft_pb2.AppendEntriesRequest(
                                term=server.currentTerm,
                                leader_id=str(server.identifier),
                                prev_log_index=server.lastApplied,
                                prev_log_term=server.currentTerm,
                                entries=[],
                                leader_commit=server.commitIndex
                            ))
                        except grpc.RpcError as e:
                            print(f"Error: {e.code()} - {e.details()}")


    def start(self):
        """Start the replica server"""
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        self.rpc_servicer = SequenceServicer(self.identifier, self.leader, self.other_replicas)
        replication_pb2_grpc.add_SequenceServicer_to_server(self.rpc_servicer, self.server)
        self.server.add_insecure_port(f'[::]:{self.identifier}')

        self._running = True

        print(self.server)
        # Start heartbeat thread -- will send heartbeats if server ever becomes primary
        self.heartbeat_thread = threading.Thread(target=self.primary_heartbeat, args=[self.rpc_servicer])
        self.heartbeat_thread.start()
        # Start watching for election timeouts
        self.election_thread = threading.Thread(target=self.election_timeout, args=[self.rpc_servicer])
        self.election_thread.start()
        # Start server
        self.server.start()
        self.server.wait_for_termination()
        self._running = False
