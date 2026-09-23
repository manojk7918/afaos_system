# AFAOS: Distributed Cognitive Agent Orchestration & Financial Audit Pipeline

AFAOS (Advanced Fault-Tolerant Agent Orchestration System) is a high-concurrency, distributed cognitive agent orchestration engine built specifically for mission-critical operations requiring absolute data integrity, resilience, and real-time observability (such as enterprise-scale financial auditing). 

The system leverages a decoupled, reactive **Split-State Architecture** to handle high-throughput operations under strict performance and fault-tolerance constraints.

---

## 🏗️ Architectural Topology Overview

```unset
                      [🐳 Docker Desktop Cluster Environments]
                                         │
       ┌─────────────────────────────────┴─────────────────────────────────┐
       ▼                                                                   ▼
[⚡ Redis Layer (RAM)]                                             [🏛️ SQLite Hard Disk Ledger]
(The High-Speed Operational State)                                 (The Secure Vault Basement)
  ├─ afaos:stream:event_ledger (Persistent Ledger Logs)               └─ afaos_audit.db
  ├─ afaos:group:orchestrator_workers (Consumer Matrix)                  └─ execution_audit_logs 
  ├─ lock:workflow:<uuid> (Distributed Concurrency Lock)
  ├─ afaos:registry:master_state (Central Registry Blueprint)
  └─ afaos:circuit:state:* (State-Machine Breaker Tokens)
                                                                  │
                                                                  │
                                      [🧠 Python Asynchronous Core (asyncio)]
                                           (The Multi-Tasking Engine Driver)
                                                                  │
                                      ┌───────────────────────────┴───────────────────────────┐
                                      ▼                                                       ▼
                            [event_broker.py]                                          [orchestrator.py]
                        (Wildcard Pattern Routers)                                (Dynamic Matrix Engine)
                          └─ psubscribe ("afaos:events:*")                          └─ SYSTEM_ROUTING_MATRIX (DAG)
                                      │                                                       │
                                      └─────────────────── [DRIFT SWEEP] ─────────────────────┘
                                                 (Automatic worker catch-up reconciliation 
                                                 if local cache != master snapshot)
```

---

## ⚡ Core Engineering Strategies & Features

### 1. Split-State Engine Architecture
* **Volatile Operational Cache (Redis)**: Drives sub-millisecond cluster state updates, transaction execution checkpoints, message routing loops, and concurrent task controls directly inside active RAM.
* **Immutable Transactional Ledger (SQLite)**: Persists unalterable relational historical execution lines, processing durations, and strict metrics schemas mapped onto local disk drives.

### 2. Directed Acyclic Graph (DAG) Routing Matrix
Coordinates isolated, multi-tenant pipelines through an strict pipeline graph sequence mapping out four specialized agent domains:
\[\text{Ingestion\_Node} \longrightarrow \text{Vector\_Indexing\_Node} \longrightarrow \text{Analysis\_Agent\_Node} \longrightarrow \text{Cloud\_Dispatch\_Node}\]

### 3. Distributed Task Recovery Engine (PEL Audit & XCLAIM)
Mitigates worker crashes midway through event processing. Implements background auditing threads that continuously monitor the Pending Entries List (**PEL**) via `XPENDING` metrics. If a message exceeds an idle timeout threshold, the broker uses `XCLAIM` to hijack the abandoned task and route it to a healthy consumer node for processing and final acknowledgment (`XACK`).

### 4. Cross-Node Dynamic State Synchronization
Prevents distributed state drifts caused by microsecond network latencies. Emits authoritative snapshot registries across a multi-topic notification channel network via wildcard pattern listeners (`psubscribe`). If an independent microservice worker detects a variance anomaly between its local metric cache counters and the master snapshot payload, it initializes a hot catch-up loop to realign cluster memory slots instantly.

### 5. Distributed Token Bucket Admission Control
Protects external dependencies and downstream LLM clusters from drowning in burst traffic. Tracks resource capacities atomically in Redis. If inbound transaction frequencies exhaust available token capacities, admission is temporarily denied, forcing an elegant, non-blocking backoff pacing delay to keep communication channels open.

### 6. State-Machine Circuit Breaker Matrix
Guarantees systemic resilience against cascading downstream failures by tracking consecutive error trends. 
* **CLOSED**: System flows normally.
* **OPEN**: Trips instantly if consecutive error bounds are crossed, short-circuiting execution frames to drop socket strain and falling back gracefully to secondary redundancy clusters.
* **HALF-OPEN**: Fires a canary test request after a short cooldown window to assess dependency health and safely determine if cluster paths can be declared healthy.

---

## 🗃️ Memory Namespace & Namespace Keys Directory

| Namespace Key Infrastructure Target | Storage Layer | Data Type Schema | System Purpose |
| :--- | :--- | :--- | :--- |
| `afaos:state:<wf_id>:<node>:status` | Redis Cache | String | Holds real-time validation execution checkpoints (`SUCCESS`, `FAILED`, `ROLLED_BACK`). |
| `afaos:stream:event_ledger` | Redis Ledger | Append-Only Stream | Stores sequential historical events for point-in-time replays. |
| `afaos:group:orchestrator_workers` | Redis Ledger | Consumer Group | Distributes processing workloads horizontally across consumer consumer fleets. |
| `afaos:queue:dead_letter` | Redis Cache | List Array Queue | Quarantines broken payloads securely for isolated forensic review. |
| `afaos:limiter:tokens:*` | Redis Cache | String | Tracks available resource capability capacities for admission pacing. |
| `afaos:circuit:state:*` | Redis Cache | String | Houses current circuit breaker state configurations (`CLOSED`, `OPEN`, `HALF_OPEN`). |
| `execution_audit_logs` | SQLite Ledger | Relational Table | Stores hard audit trails and sub-second calculation metrics logs. |

---

## 🚀 Getting Started

### Prerequisites
* Python 3.10+
* Docker Desktop & Redis-Stack-Server instance active
* Asynchronous drivers dependency array (`pip install redis-py sqlite3-builtin-mock-layers`)

### System Verification Run
Execute the main pipeline drive loop using your terminal console:
```bash
python3 orchestrator.py
```

An enterprise-grade, fault-tolerant orchestration platform designed to manage high-concurrency AI agent lifecycles over multi-layered distributed states.
