import raft_pb2
import json

def log_entry_to_dict(entry: raft_pb2.LogEntry) -> dict:
    return { 
        "index":  entry.index,
        "term":   entry.term,
        "opcode": entry.opcode,
        "key":    entry.key,
        "val":    entry.val
    }

def dict_to_log_entry(entry: dict) -> raft_pb2.LogEntry:
    return raft_pb2.LogEntry(
        index  = entry["index"],
        term   = entry["term"],
        opcode = entry["opcode"],
        key    = entry["key"],
        val    = entry["val"]
    )
