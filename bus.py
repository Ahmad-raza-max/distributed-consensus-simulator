# src/network/bus.py
import queue
import random
import threading
import time
from typing import Callable, Dict, Optional, Set, Tuple

from rpc import RPCMessage


class NetworkBus:
    """In-memory message router supporting latency, drops, and partitions."""

    def __init__(self, drop_rate: float = 0.0, base_latency_ms: float = 20.0):
        self.drop_rate: float = drop_rate
        self.base_latency_ms: float = base_latency_ms

        # node_id -> inbox queue
        self._inboxes: Dict[int, queue.Queue] = {}

        # node_id -> handler callback function (node.receive_rpc)
        self._handlers: Dict[int, Callable[[RPCMessage], None]] = {}

        # Network partitions: set of active isolates or partitioned pairs
        # Format: {(node_a, node_b)} implies connection between a and b is severed
        self._severed_links: Set[Tuple[int, int]] = set()

        self._lock = threading.Lock()
        self._running = True

    def register_node(
        self, node_id: int, handler: Optional[Callable[[RPCMessage], None]] = None
    ) -> queue.Queue:
        """Register a node to receive messages via inbox queue or direct callback."""
        with self._lock:
            q: queue.Queue = queue.Queue()
            self._inboxes[node_id] = q
            if handler:
                self._handlers[node_id] = handler
            return q

    def deregister_node(self, node_id: int) -> None:
        """Remove a node when stopped or crashed."""
        with self._lock:
            self._inboxes.pop(node_id, None)
            self._handlers.pop(node_id, None)

    # --- Chaos & Fault Injection API ---

    def isolate_node(self, node_id: int) -> None:
        """Sever all incoming and outgoing connections for a specific node."""
        with self._lock:
            for peer in self._inboxes:
                if peer != node_id:
                    self._severed_links.add((node_id, peer))
                    self._severed_links.add((peer, node_id))

    def heal_node(self, node_id: int) -> None:
        """Restore all severed connections for a node."""
        with self._lock:
            self._severed_links = {
                link for link in self._severed_links if node_id not in link
            }

    def partition_cluster(self, group_a: Set[int], group_b: Set[int]) -> None:
        """Create a split-brain partition between two groups of nodes."""
        with self._lock:
            for a in group_a:
                for b in group_b:
                    self._severed_links.add((a, b))
                    self._severed_links.add((b, a))

    def heal_all_partitions(self) -> None:
        """Reconnect all partitioned nodes."""
        with self._lock:
            self._severed_links.clear()

    # --- Transmission Logic ---

    def send(self, message: RPCMessage) -> None:
        """Schedule message transmission in a separate background worker."""
        if not self._running:
            return

        # Start non-blocking thread for delivery simulation
        delivery_thread = threading.Thread(
            target=self._deliver_worker,
            args=(message,),
            daemon=True,
        )
        delivery_thread.start()

    def _deliver_worker(self, msg: RPCMessage) -> None:
        sender = msg.sender_id
        receiver = msg.receiver_id

        # 1. Check network partition / severed links
        with self._lock:
            if (sender, receiver) in self._severed_links:
                # Silently drop packet due to partition
                return

        # 2. Simulate packet loss
        if self.drop_rate > 0.0 and random.random() < self.drop_rate:
            # Packet dropped by noisy channel
            return

        # 3. Simulate network latency + jitter
        if self.base_latency_ms > 0:
            jitter = random.uniform(0.7, 1.3)
            delay_sec = (self.base_latency_ms * jitter) / 1000.0
            time.sleep(delay_sec)

        # 4. Dispatch to receiver
        with self._lock:
            inbox = self._inboxes.get(receiver)
            handler = self._handlers.get(receiver)

        if handler:
            try:
                handler(msg)
            except Exception as e:
                print(f"[NetworkBus Error] Failed delivering to Node {receiver}: {e}")
        elif inbox:
            inbox.put(msg)

    def shutdown(self) -> None:
        self._running = False