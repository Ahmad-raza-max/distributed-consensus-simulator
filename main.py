# main.py
import os
import sys
import time
from typing import Dict, List

from bus import NetworkBus
from node import RaftNode


def print_status_table(nodes: Dict[int, RaftNode]) -> None:
    print("\n" + "=" * 55)
    print(f"{'Node ID':<10} | {'Role':<12} | {'Term':<8} | {'Status':<10}")
    print("-" * 55)
    for node_id, node in nodes.items():
        role_display = node.role
        if node.role == "Leader":
            role_display = "👑 LEADER"
        status_display = "ALIVE" if node.alive else "CRASHED"
        print(f"{node_id:<10} | {role_display:<12} | {node.current_term:<8} | {status_display:<10}")
    print("=" * 55 + "\n")


def main():
    print("🚀 Initializing 5-Node Distributed Consensus Cluster...\n")

    # In-memory bus with 15ms simulated network latency
    bus = NetworkBus(drop_rate=0.0, base_latency_ms=15.0)

    cluster_size = 5
    all_ids = list(range(1, cluster_size + 1))
    nodes: Dict[int, RaftNode] = {}

    # Initialize all nodes
    for node_id in all_ids:
        peer_ids = [pid for pid in all_ids if pid != node_id]
        nodes[node_id] = RaftNode(node_id=node_id, peer_ids=peer_ids, bus=bus)

    # Start all nodes
    for node in nodes.values():
        node.start()

    print("✅ Sabhi nodes active hain. Election ka wait kar rahe hain...\n")

    # Monitor election phase
    time.sleep(4)
    print_status_table(nodes)

    # Find the current leader
    current_leader = next((n for n in nodes.values() if n.role == "Leader"), None)

    if current_leader:
        print(f"💥 CHAOS TEST: Leader (Node {current_leader.node_id}) ko crash kar rahe hain...")
        current_leader.stop()

        print("⏳ Dekhte hain baaki 4 nodes naya leader elect karte hain ya nahi...\n")
        time.sleep(4)

        print_status_table(nodes)

    print("Simulation finished cleanly.")
    bus.shutdown()


if __name__ == "__main__":
    main()