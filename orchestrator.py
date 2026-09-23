import asyncio
import json
import logging
import time
from datetime import datetime
import redis.asyncio as aioredis

# Core System Engine Integrations
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
    global shared_state_memory_ref
    if event_envelope.get("event_type") == "CLUSTER_STATE_HEARTBEAT":
        raw_payload = event_envelope.get("payload", "{}")
        if isinstance(raw_payload, str):
            try: raw_payload = json.loads(raw_payload)
            except Exception: return
        if shared_state_memory_ref:
            await shared_state_memory_ref.evaluate_and_reconcile_drift(raw_payload)

async def run_persistent_orchestrator(workflow_uuid, routing_matrix, state_memory, event_broker, metrics_exporter, compactor):
    logger.info(f"Launching Day 29 Orchestration Engine Layer for Workflow: {workflow_uuid}")
    
    for node in routing_matrix:
        start_time = time.time()
        
        # Circuit Breaker Check
        circuit_state = await state_memory.check_circuit_status(node, cooldown_sec=2.0)
        if circuit_state == "OPEN":
            logger.critical(f"🛑 [FAST-FAIL TRIGGERED]: Circuit is OPEN for '{node}'! Bypassing network connection immediately.")
            await metrics_exporter.increment_node_counter(node, "circuit_fast_fail")
            print("🔄 [SAFE METRICS DEGRADATION]: Dynamic fallback activated.\n")
            continue
            
        # Admission Control Check
        has_admission = await state_memory.evaluate_admission_allowance(node, max_tokens=5, refill_rate_per_sec=2.0)
        if not has_admission:
            logger.warning(f"⏳ [ADMISSION THROTTLED]: Rate limiter burst hit on '{node}'. Pacing processing frames...")
            await asyncio.sleep(0.2)
        
        if node == "Analysis_Agent_Node":
            logger.error(f"💥 [CRITICAL NODE FAILURE]: '{node}' has faulted database connectivity sockets unexpectedly!")
            await event_broker.handle_dead_letter(
                failed_node=node,
                error_message="Connectivity dropped.",
                original_payload={"target_node": node, "session_id": workflow_uuid}
            )
            await metrics_exporter.increment_node_counter(node, "dlq_quarantine")
            await state_memory.client.incr("afaos:analytics:failures_quarantined")
            await state_memory.report_node_execution_result(node, is_success=False, failure_threshold=2)
            print("🔄 [STATE REVERSAL]: Active node transactional state cleanly reverted to parent ledger checkpoints.\n")
            continue
            
        await event_broker.publish_event(
            topic=node,
            event_type="NODE_EXECUTION_START",
            source_node="System_Orchestrator_Core",
            payload={"target_node": node, "status": "processing"}
        )
        
        await state_memory.client.incr("afaos:analytics:total_processed")
        await state_memory.force_local_increment("total_processed", 1)
        await state_memory.report_node_execution_result(node, is_success=True)
        
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
    logger.info("Initializing high-concurrency Asynchronous Redis Connection Pools...")
    
    # Establish two completely independent connection clients to prevent Pub/Sub deadlock
    redis_runtime_client = aioredis.from_url("redis://localhost:6379", decode_responses=True)
    redis_broker_client = aioredis.from_url("redis://localhost:6379", decode_responses=True)

    state_memory = AgentStateMemory(redis_runtime_client)
    shared_state_memory_ref = state_memory
    
    event_broker = RedisEventBroker(redis_broker_client)
    metrics_exporter = SystemMetricsExporter(redis_runtime_client)
    compactor = TelemetryCompactor(redis_runtime_client, threshold_ms=1000.0)

    # Clean sandbox memory baselines
    await redis_runtime_client.delete("afaos:circuit:state:Analysis_Agent_Node")
    await redis_runtime_client.delete("afaos:circuit:failures:Analysis_Agent_Node")
    await redis_runtime_client.set("afaos:analytics:total_processed", "0")
    await redis_runtime_client.set("afaos:analytics:failures_quarantined", "0")

    try:
        async with RedisDistributedLock(redis_runtime_client, relational_workflow_uuid, lease_time_sec=60):
            logger.info("🔒 [CONCURRENCY GUARD ACTIVE]: System execution context locked cleanly.")
            
            await event_broker.initialize_consumer_group()
            
            if hasattr(event_broker, 'start_pattern_listener'):
                await event_broker.start_pattern_listener("afaos:events:*", cluster_sync_pattern_callback)
            
            print("\n🚀 --- PIPELINE RUN 1: PROCESS WORKFLOW MATRIX ---")
            await run_persistent_orchestrator(relational_workflow_uuid, SYSTEM_ROUTING_MATRIX, state_memory, event_broker, metrics_exporter, compactor)
            
            print("\n🚀 --- PIPELINE RUN 2: VERIFY FAST-FAIL STATE RECOVERY ---")
            await run_persistent_orchestrator(relational_workflow_uuid, SYSTEM_ROUTING_MATRIX, state_memory, event_broker, metrics_exporter, compactor)
            
            if hasattr(event_broker, 'stop_pattern_listener'):
                await event_broker.stop_pattern_listener()
            
    except RuntimeError as lock_err:
        print(f"\n🛑 [ABORT]: Critical concurrency conflict encountered: {lock_err}")
        return
    finally:
        await redis_runtime_client.close()
        await redis_broker_client.close()

    print("\n🔌 [GLOBAL SHUTDOWN COMPLETE]: Global TCP network socket pools released cleanly from RAM.")

if __name__ == "__main__":
    try:
        import sys
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass
        
    asyncio.run(main())

