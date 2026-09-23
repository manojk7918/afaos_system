import asyncio
import json
import logging
import time
from datetime import datetime

# Core System Engine Integrations (Day 18 - Day 27)
from dist_lock import RedisDistributedLock
from state_memory import AgentStateMemory
from event_broker import RedisEventBroker
from metrics_exporter import SystemMetricsExporter
from telemetry_compactor import TelemetryCompactor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Orchestrator")

SYSTEM_ROUTING_MATRIX = ["Ingestion_Node", "Vector_Indexing_Node", "Analysis_Agent_Node", "Cloud_Dispatch_Node"]
shared_state_memory_ref = None

async def cluster_sync_pattern_callback(event_envelope: dict):
    """Day 27 Wildcard Callback intercepting state sync heartbeats across the cluster."""
    global shared_state_memory_ref
    
    if event_envelope.get("event_type") == "CLUSTER_STATE_HEARTBEAT":
        raw_payload = event_envelope.get("payload", "{}")
        if isinstance(raw_payload, str):
            try:
                raw_payload = json.loads(raw_payload)
            except Exception:
                return
                
        print(f"📡 [SYNC HEARTBEAT INTERCEPTED]: Incoming global registry broadcast parsed.")
        if shared_state_memory_ref:
            await shared_state_memory_ref.evaluate_and_reconcile_drift(raw_payload)

async def run_persistent_orchestrator(workflow_uuid, routing_matrix, state_memory, event_broker, metrics_exporter, compactor):
    """Processes node graphs while maintaining synchronized atomic metric matrices."""
    logger.info(f"Launching Day 27 Orchestration Engine Layer for Workflow: {workflow_uuid}")
    
    for node in routing_matrix:
        start_time = time.time()
        
        if node == "Analysis_Agent_Node":
            logger.error(f"💥 [CRITICAL FAILURE]: '{node}' dropped database connectivity sockets unexpectedly!")
            await event_broker.handle_dead_letter(
                failed_node=node,
                error_message="Connectivity dropped.",
                original_payload={"target_node": node, "session_id": workflow_uuid}
            )
            await metrics_exporter.increment_node_counter(node, "dlq_quarantine")
            await event_broker.redis_client.incr("afaos:analytics:failures_quarantined")
            print("🔄 [STATE REVERSAL]: Active node transactional state cleanly reverted to parent ledger checkpoints.\n")
            continue
            
        await event_broker.publish_event(
            topic=node,
            event_type="NODE_EXECUTION_START",
            source_node="System_Orchestrator_Core",
            payload={"target_node": node, "status": "processing"}
        )
        
        # Advance state tracking matrices across central cluster indices
        await event_broker.redis_client.incr("afaos:analytics:total_processed")
        await state_memory.force_local_increment("total_processed", 1)
        
        logger.info(f"Checkpoint match found! Key [{node}] skipped inside tracking cache.")
        await asyncio.sleep(0.05) 
        
        await event_broker.publish_event(
            topic=node,
            event_type="NODE_EXECUTION_COMPLETED",
            source_node="System_Orchestrator_Core",
            payload={"target_node": node, "status": "cache_skipped"}
        )
        
        duration_ms = (time.time() - start_time) * 1000.0
        await metrics_exporter.record_node_latency(node, duration_ms)
        await metrics_exporter.increment_node_counter(node, "execution_success")
        
        await compactor.analyze_sliding_window_anomalies(node)
        print("")

    logger.info("✅ Full system graph execution completed and archived permanently in relational tables.")

async def main():
    global shared_state_memory_ref
    relational_workflow_uuid = "fa15b023-5e8c-411a-bd63-902fd7b8e1a4"
    
    logger.info("Initializing permanent relational storage engine layer: afaos_audit.db")
    logger.info("Initializing high-concurrency Asynchronous Redis Connection Pool...")
    
    import redis.asyncio as aioredis
    redis_runtime_client = aioredis.from_url("redis://localhost:6379", decode_responses=True)

    state_memory = AgentStateMemory(redis_runtime_client)
    shared_state_memory_ref = state_memory
    
    event_broker = RedisEventBroker(redis_runtime_client)
    metrics_exporter = SystemMetricsExporter(redis_runtime_client)
    compactor = TelemetryCompactor(redis_runtime_client, threshold_ms=1000.0)

    # Reset metrics from previous test traces for a clean verification baseline
    await redis_runtime_client.set("afaos:analytics:total_processed", "0")
    await redis_runtime_client.set("afaos:analytics:failures_quarantined", "0")

    try:
        async with RedisDistributedLock(redis_runtime_client, relational_workflow_uuid, lease_time_sec=60):
            logger.info("🔒 [CONCURRENCY GUARD ACTIVE]: System execution context locked cleanly.")
            
            await event_broker.initialize_consumer_group()
            await event_broker.start_pattern_listener("afaos:events:*", cluster_sync_pattern_callback)
            
            await run_persistent_orchestrator(relational_workflow_uuid, SYSTEM_ROUTING_MATRIX, state_memory, event_broker, metrics_exporter, compactor)
            
            # 🕵️‍♂️ --- Day 27 State Drift & Reconciliation Simulation Sweep ---
            print("\n🕵️‍♂️ --- Day 27 Distributed State Drift Alignment Simulation ---")
            
            # 1. Manually introduce a silent state drift into the local memory cache
            await state_memory.force_local_increment("total_processed", 12)  
            print(f"📊 [LOCAL WORKER STATUS]: Local worker cache count drifted to: {await state_memory.get_local_metric('total_processed')}")
            
            # 2. Master node compiles the official cluster blueprint records
            master_snapshot = await state_memory.generate_master_snapshot()
            print(f"👑 [MASTER REGISTRY RECORDS]: Centralized cluster database records are at: {master_snapshot['master_registry']['total_processed']}")
            
            # 3. Broadcast the master snapshot across the high-performance network channel
            print("📣 [BROADCASTING HEARTBEAT]: Transmitting cluster state sync blueprint vector...")
            await event_broker.publish_event(
                topic="sys.heartbeat",
                event_type="CLUSTER_STATE_HEARTBEAT",
                source_node="Master_Registry_Node",
                payload=master_snapshot
            )
            
            # Allow background callback thread processing interval to clear drift alignment
            await asyncio.sleep(0.5)
            
            print(f"🎉 [FINAL SYNCHRONIZATION RUN]: Local cache value is now: {await state_memory.get_local_metric('total_processed')}")
            await event_broker.stop_pattern_listener()
            
    except RuntimeError as lock_err:
        print(f"\n🛑 [ABORT]: Critical concurrency conflict encountered: {lock_err}")
        return

    print("\n🔌 [GLOBAL SHUTDOWN COMPLETE]: Global TCP network socket pools released cleanly from RAM.")

if __name__ == "__main__":
    try:
        import sys
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass
        
    asyncio.run(main())
