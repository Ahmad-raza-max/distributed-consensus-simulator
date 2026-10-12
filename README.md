# ⚡ Distributed Consensus Simulator (Raft Protocol)

An in-memory simulation of the Raft distributed consensus algorithm built from scratch in Python. It demonstrates leader election, heartbeat mechanism, fault tolerance, and automatic failover under network anomalies.

## 🚀 Key Features
- **Leader Election:** Randomized timeouts prevent split votes and elect a cluster leader via majority consensus (N/2 + 1).
- **In-Memory Network Bus:** Virtual packet router supporting simulated network latency and message delivery.
- **Chaos & Fault Tolerance:** Automatic node crash simulation and seamless failover to a newly elected leader without manual intervention.

## 🛠️ Project Structure
- `rpc.py`: RPC message definitions (RequestVote, AppendEntries).
- `bus.py`: Virtual network broker (latency & message delivery).
- `node.py`: Raft state machine (Follower, Candidate, Leader logic).
- `main.py`: 5-node cluster setup and chaos injection workflow.

## 💻 How to Run
1. Clone the repository:
   ```bash
   git clone [https://github.com/Ahmad-raza-max/distributed-consensus-simulator.git](https://github.com/Ahmad-raza-max/distributed-consensus-simulator.git)
   cd distributed-consensus-simulator
   python main.py
   '''
