import grpc
import threading
import time
from concurrent import futures
import replication_pb2
import replication_pb2_grpc
import raft_pb2
import raft_pb2_grpc
import random
import redis
# import heartbeat_service_pb2
# import heartbeat_service_pb2_grpc

from constants import KNOWN_REPLICAS

# Seconds per heartbeat
# HEARTBEAT_RATE = 5

NAK="NAK"
ACK="ACK"


class SequenceServicer(replication_pb2_grpc.SequenceServicer):
    def __init__(self, identifier: int, leader: int, replicas: list):
        self.db = redis.Redis(host='localhost', port=(identifier + 100), decode_responses=True)
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
        self.nextIndex  = { rep: self.commitIndex + 1 for rep in replicas if not rep == self.identifier }  # pair of (server index, next log index)
        self.matchIndex = { rep: 0                    for rep in replicas if not rep == self.identifier }  # pair of (server index, highest log entry replicated)

        self.lastHeartbeat = time.time_ns()

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
                # print("next index:", self.nextIndex)
                # print("target:", self.nextIndex[rep_id])
                # print("logs:", self.log.values())
                new_entries = [ entry for entry in self.log.values() if entry.index >= self.nextIndex[rep_id] ]
                # print(type(new_entries[0]))
                # print(type(self.log[1]))
                if not new_entries: break

                with grpc.insecure_channel(f"localhost:{rep_id}") as channel:
                    print(f"{self.identifier} (leader): Writing to {rep_id} @ index {self.nextIndex[rep_id]}.")
                    print(f"Sending entries: {list(new_entries)}")
                    replica_stub = replication_pb2_grpc.SequenceStub(channel)
                    try:
                        not_stupid = raft_pb2.AppendEntriesRequest(
                                    term=self.currentTerm,
                                    leader_id=f"{self.identifier}",
                                    prev_log_index=self.nextIndex[rep_id] - 1,
                                    prev_log_term=self.log[self.nextIndex[rep_id]].term,
                                    entries=new_entries,
                                    leader_commit=self.commitIndex
                                )
                        res = replica_stub.AppendEntries(not_stupid)
                        if res.success:
                            # TODO this is sussy
                            print(f"AppendEntries succeeded for {rep_id}!")
                            self.nextIndex[rep_id]  = self.commitIndex + 2
                            self.matchIndex[rep_id] = self.commitIndex + 1
                            break
                        else:
                            # If AppendEntries fails, decrement nextIndex and try again
                            print(f"AppendEntries failed for {rep_id}! Decrementing nextIndex...")
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
        for i in range(max(self.matchIndex.values()), self.commitIndex, -1):
            numReplicated = len([ n for n in self.matchIndex.values() if n >= i ])
            if numReplicated >= len(self.replicas) // 2 and self.log[i].term == self.currentTerm:
                self.commitIndex = i
                print(f"Consensus reached: committing index {i}.")
                print(f"Log is now {self.log}")
                break
        else:
            print("No consensus.")

        return replication_pb2.WriteResponse(ack=ACK)

    def Read(self, req, ctx):
        # If not leader, redirect:
        if self.leader != self.identifier:
            with grpc.insecure_channel(f"localhost:{self.leader}") as channel:
                leader_stub = replication_pb2_grpc.SequenceStub(channel)
                print(f"{self.identifier}: Not leader, redirecting to leader (f{self.leader})...")
                try:
                    return leader_stub.Read(req)
                except grpc.RpcError as e:
                    # Forward error to client
                    if e.code() == grpc.StatusCode.UNAVAILABLE:
                        ctx.set_code(grpc.StatusCode.UNAVAILABLE)
                    elif e.code() == grpc.StatusCode.NOT_FOUND:
                        ctx.set_code(grpc.StatusCode.NOT_FOUND)
                    # Otherwise I don't know the error, so crash:
                    else: raise e
        else:
            print(f"{self.identifier}: Reading {req.key}...")
            if self.db.exists(req.key):
                return replication_pb2.ReadResponse(value=self.db.get(req.key))
            else:
                ctx.set_code(grpc.StatusCode.NOT_FOUND)

    def _delete_after_index(self, index: int):
        for i in self.log:
            if i >= index: del self.log[i]

    def AppendEntries(self, req, ctx):
        if req.entries:
            print("New AppendEntries request:")
        # I received something from a leader, acknowledge that leader instead
        self.lastHeartbeat = time.time_ns()
        if self.leader != int(req.leader_id):
            print(f"{self.identifier}: Changing leader to {req.leader_id}")
            self.leader = int(req.leader_id)
            self.currentTerm = req.term
        #print(f"{self.identifier}: received heartbeat from {self.leader}")

        # if not req.entries:
        #     return raft_pb2.AppendEntriesResponse(term=self.currentTerm, success=True)

        # Reply false if term < currentTerm
        if req.term < self.currentTerm:
            print(f"{self.identifier}: {req.term} (req.term) < {self.currentTerm} (currentTerm)")
            return raft_pb2.AppendEntriesResponse(term=self.currentTerm, success=False)

        # Reply false if log doesn’t contain an entry at prevLogIndex whose term matches
        if req.prev_log_index > 0 and (req.prev_log_index not in self.log or self.log[req.prev_log_index].term != req.prev_log_term):
            # print(f"{self.identifier}: Log does not contain entry at {req.prev_log_index} with term {req.prev_log_term}")
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
                print(f"Appended new entry to log: {entry.opcode} {entry.key} {entry.val}")

        # If leaderCommit > commitIndex, set:
        #     commitIndex = min(leaderCommit, index of last new entry)
        if req.leader_commit > self.commitIndex:
            self.commitIndex = min(req.leader_commit, list(self.log)[-1] if self.log else 0)
            print(f"{self.identifier}: Commited up to {self.commitIndex}")
            print(f"{self.identifier}: Log is now {self.log}")

        return raft_pb2.AppendEntriesResponse(term=self.currentTerm, success=True)

    def RequestVote(self, request, context):
        # Reset election timeout on vote request
        self.lastHeartbeat = time.time_ns()

        # If request is from an older term, reject it
        if request.term < self.currentTerm:
            return raft_pb2.RequestVoteResponse(term=self.currentTerm, vote_granted=False)
        
        # If I haven't voted yet, vote for the candidate
        if (self.votedFor is None or self.votedFor == request.candidate_id) and \
            (request.last_log_index >= list(self.log)[-1] if self.log else 0):
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
        self.timeout = random.randrange(150, 300) * 1000000 * 10 # 150-300ms converted to ns
        self.heartbeat_rate = 50000000 * 10 # 50ms converted to ns
    
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
                        last_log_index=list(server.log)[-1] if server.log else 0,
                    ))
                    if res.vote_granted:
                        votes += 1
                except grpc.RpcError as e:
                    if e.code() != grpc.StatusCode.UNAVAILABLE and not (e.code() == grpc.StatusCode.INTERNAL and "serialize" in e.details()):
                        print(f"Vote Request Error: {e.code()} - {e.details()}")
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
                            prev_log_index=list(server.log)[-1] if server.log else 0,
                            prev_log_term=server.currentTerm,
                            entries=[],
                            leader_commit=server.commitIndex
                        ))
                    except grpc.RpcError as e:
                        if e.code() != grpc.StatusCode.UNAVAILABLE and not (e.code() == grpc.StatusCode.INTERNAL and "serialize" in e.details()):
                            print(f"Leader Confirmation Error: {e.code()} - {e.details()}")

            # Reset leader states
            server.nextIndex = { replica: list(server.log)[-1] + 1 if server.log else 1 for replica in server.replicas if replica != server.identifier}
            server.matchIndex = { replica: 0 for replica in server.replicas if replica != server.identifier}
            print(f"{server.identifier}: Elected primary for term {server.currentTerm}.")


    def election_timeout(self, server):
        """Election timeout for Raft"""
        while self._running:
            # Ignore election timeout if we're the primary
            if server.leader == server.identifier: continue
            if time.time_ns() - server.lastHeartbeat > self.timeout:
                server.lastHeartbeat = time.time_ns()
                print(f"Primary ({server.leader}) timed out. Starting election.")
                self.start_election(server)
            # If we're a backup, we need to start an election

    def primary_heartbeat(self, server):
        """heartbeat"""
        while self._running:
            # Only run if leader
            if server.leader != server.identifier: continue 

            # Send heartbeat to all replicas every heartbeat_rate
            if time.time_ns() - server.lastHeartbeat >= self.heartbeat_rate:
                server.lastHeartbeat = time.time_ns()
                for replica in server.replicas:
                    if replica == server.identifier: continue
                    #print(f"{server.identifier} (primary): sending heartbeat to {replica}.")
                    with grpc.insecure_channel(f"localhost:{replica}") as channel:
                        replica_server = replication_pb2_grpc.SequenceStub(channel)
                        try:
                            res = replica_server.AppendEntries(raft_pb2.AppendEntriesRequest(
                                term=server.currentTerm,
                                leader_id=str(server.identifier),
                                prev_log_index=list(server.log)[-1] if server.log else 0,
                                prev_log_term=server.currentTerm,
                                entries=[],
                                leader_commit=server.commitIndex
                            ))
                        except grpc.RpcError as e:
                            if e.code() != grpc.StatusCode.UNAVAILABLE and not (e.code() == grpc.StatusCode.INTERNAL and "serialize" in e.details()):
                                print(f"Heartbeat Error: {e.code()} - {e.details()}")


    def start(self):
        """Start the replica server"""
        try:
            self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            self.rpc_servicer = SequenceServicer(self.identifier, self.leader, self.other_replicas)
            replication_pb2_grpc.add_SequenceServicer_to_server(self.rpc_servicer, self.server)
            self.server.add_insecure_port(f'[::]:{self.identifier}')

            self._running = True

            # Start heartbeat thread -- will send heartbeats if server ever becomes primary
            self.heartbeat_thread = threading.Thread(target=self.primary_heartbeat, args=[self.rpc_servicer])
            self.heartbeat_thread.start()
            # Start watching for election timeouts
            self.election_thread = threading.Thread(target=self.election_timeout, args=[self.rpc_servicer])
            self.election_thread.start()
            # Start server
            self.server.start()
            self.server.wait_for_termination()
        except KeyboardInterrupt:
            print("\nShutting down...")
        finally:
            self._running = False
