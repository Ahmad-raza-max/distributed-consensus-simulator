# node.py
import random
import threading
import time
from typing import Dict, List, Optional

from bus import NetworkBus
from rpc import (
    AppendEntriesArgs,
    AppendEntriesReply,
    RequestVoteArgs,
    RequestVoteReply,
    RPCMessage,
)


class RaftNode:
    def __init__(self, node_id: int, peer_ids: List[int], bus: NetworkBus):
        self.node_id: int = node_id
        self.peer_ids: List[int] = peer_ids  # IDs of all other nodes
        self.bus: NetworkBus = bus

        # Raft state
        self.role: str = "Follower"  # Follower, Candidate, Leader
        self.current_term: int = 0
        self.voted_for: Optional[int] = None
        self.votes_received: int = 0

        # Timing controls
        self.last_heartbeat_time: float = time.time()
        self.election_timeout: float = self._get_random_timeout()
        self.heartbeat_interval: float = 0.5  # Leader sends heartbeat every 500ms

        self.alive: bool = True
        self.lock = threading.Lock()

        # Register callback with network bus
        self.bus.register_node(self.node_id, handler=self.receive_rpc)

    def _get_random_timeout(self) -> float:
        # 1.5 to 3.0 seconds randomized window to prevent split votes
        return random.uniform(1.5, 3.0)

    def reset_election_timer(self) -> None:
        self.last_heartbeat_time = time.time()
        self.election_timeout = self._get_random_timeout()

    def start(self) -> None:
        """Start the node loop in a background thread."""
        t = threading.Thread(target=self._run_loop, daemon=True)
        t.start()

    def stop(self) -> None:
        """Simulate a node crash."""
        with self.lock:
            self.alive = False
            self.role = "Dead"

    # --- Core Event Loop ---

    def _run_loop(self) -> None:
        while self.alive:
            time.sleep(0.1)

            with self.lock:
                now = time.time()

                if self.role == "Leader":
                    # Leader sends periodic heartbeats
                    if now - self.last_heartbeat_time >= self.heartbeat_interval:
                        self._send_heartbeats()
                        self.last_heartbeat_time = now

                else:
                    # Follower or Candidate: check if election timeout expired
                    if now - self.last_heartbeat_time >= self.election_timeout:
                        self._start_election()

    # --- Election Actions ---

    def _start_election(self) -> None:
        self.role = "Candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = 1  # Vote for self
        self.reset_election_timer()

        print(
            f"⚡ [Node {self.node_id}] Timeout! Candidate bana, Term {self.current_term} start kiya."
        )

        # Request votes from all peers
        for peer_id in self.peer_ids:
            req = RequestVoteArgs(
                sender_id=self.node_id,
                receiver_id=peer_id,
                term=self.current_term,
                candidate_id=self.node_id,
            )
            self.bus.send(req)

    def _send_heartbeats(self) -> None:
        for peer_id in self.peer_ids:
            heartbeat = AppendEntriesArgs(
                sender_id=self.node_id,
                receiver_id=peer_id,
                term=self.current_term,
                leader_id=self.node_id,
            )
            self.bus.send(heartbeat)

    # --- Inbound Message Handler (Callback from NetworkBus) ---

    def receive_rpc(self, msg: RPCMessage) -> None:
        with self.lock:
            if not self.alive:
                return

            # Term synchronization: agar kisi message me higher term mile, drop to follower
            if hasattr(msg, "term") and msg.term > self.current_term:
                self.current_term = msg.term
                self.role = "Follower"
                self.voted_for = None

            # Route by message type
            if isinstance(msg, RequestVoteArgs):
                self._handle_request_vote(msg)
            elif isinstance(msg, RequestVoteReply):
                self._handle_vote_reply(msg)
            elif isinstance(msg, AppendEntriesArgs):
                self._handle_append_entries(msg)
            elif isinstance(msg, AppendEntriesReply):
                pass

    def _handle_request_vote(self, req: RequestVoteArgs) -> None:
        vote_granted = False

        # Grant vote if sender term >= current_term AND we haven't voted yet
        if req.term >= self.current_term:
            if self.voted_for is None or self.voted_for == req.candidate_id:
                vote_granted = True
                self.voted_for = req.candidate_id
                self.reset_election_timer()

        reply = RequestVoteReply(
            sender_id=self.node_id,
            receiver_id=req.sender_id,
            term=self.current_term,
            vote_granted=vote_granted,
        )
        self.bus.send(reply)

    def _handle_vote_reply(self, reply: RequestVoteReply) -> None:
        # Sirf tab count karo agar node abhi bhi candidate hai aur same term hai
        if self.role == "Candidate" and reply.term == self.current_term:
            if reply.vote_granted:
                self.votes_received += 1
                total_nodes = len(self.peer_ids) + 1
                majority = (total_nodes // 2) + 1

                if self.votes_received >= majority:
                    self.role = "Leader"
                    print(
                        f"\n👑 >>> [Node {self.node_id}] LEADER ban gaya! (Term {self.current_term}, Votes: {self.votes_received}/{total_nodes}) <<<\n"
                    )
                    self._send_heartbeats()
                    self.last_heartbeat_time = time.time()

    def _handle_append_entries(self, req: AppendEntriesArgs) -> None:
        # Leader se valid heartbeat aayi
        if req.term >= self.current_term:
            self.role = "Follower"
            self.current_term = req.term
            self.reset_election_timer()

        reply = AppendEntriesReply(
            sender_id=self.node_id,
            receiver_id=req.sender_id,
            term=self.current_term,
            success=(req.term >= self.current_term),
        )
        self.bus.send(reply)