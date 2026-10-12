# src/network/rpc.py
from dataclasses import dataclass, field
from typing import Any, List, Optional
import time
import uuid


@dataclass
class LogEntry:
    term: int
    index: int
    command: Any  # e.g., {"key": "x", "val": 10}


@dataclass
class RPCMessage:
    msg_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    sender_id: int = 0
    receiver_id: int = 0
    timestamp: float = field(default_factory=time.time)


# --- Leader Election RPCs ---

@dataclass
class RequestVoteArgs(RPCMessage):
    term: int = 0
    candidate_id: int = 0
    last_log_index: int = 0
    last_log_term: int = 0


@dataclass
class RequestVoteReply(RPCMessage):
    term: int = 0
    vote_granted: bool = False


# --- Log Replication / Heartbeat RPCs ---

@dataclass
class AppendEntriesArgs(RPCMessage):
    term: int = 0
    leader_id: int = 0
    prev_log_index: int = 0
    prev_log_term: int = 0
    entries: List[LogEntry] = field(default_factory=list)
    leader_commit: int = 0


@dataclass
class AppendEntriesReply(RPCMessage):
    term: int = 0
    success: bool = False
    match_index: int = 0