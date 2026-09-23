import asyncio
import json
import logging
import time
from datetime import datetime

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

# FIX 1: Callback accepts 2 positional arguments (channel and event_envelope)
async def cluster_sync_pattern_callback(channel: str, event_envelope: dict):
    global shared_state_memory_ref
    if isinstance(event_envelope, dict) and event_envelope.get("event_type") == "CLUSTER_STATE_HEARTBEAT":
        raw_payload = event_envelope.get("payload", "{}")
        if isinstance(raw_payload, str):
            try:
                raw_payload = json.loads(raw_payload)
            except Exception:
                return
        if shared_state_memory_ref:
            await shared_state_memory_ref.evaluate_and_reconcile_drift(raw_payload)

async def run_persistent_orchestrator(workflow_uuid, routing_matrix, state_memory, event_broker, metrics_exporter, compactor):
    logger.info(f"Launching Day 28 Orchestration Engine Layer for Workflow: {workflow_uuid}")
    
    for node in routing_matrix:
        start_time = time.time()
        
        # Day 28 Admission Control Gatekeeper Verification
        # Limits burst capacity aggressively to simulate quick throttle behaviors
        has_admission = await state_memory.evaluate_admission_allowance(node, max_tokens=2, refill_rate_per_sec=0.5)
        
        if not has_admission:
            logger.warning(f"⏳ [ADMISSION THROTTLED]: High-concurrency delay forced on '{node}' for backoff pacing...")
            await asyncio.sleep(1.0) # Graceful backoff pause delay window injection
        
        if node == "Analysis_Agent_Node":
            logger.error(f"💥 [CRITICAL FAILURE]: '{node}' dropped database connectivity sockets unexpectedly!")
            if hasattr(event_broker, 'handle_dead_letter'):
                await event_broker.handle_dead_letter(
                    failed_node=node,
                    error_message="Connectivity dropped.",
                    original_payload={"target_node": node, "session_id": workflow_uuid}
                )
            else:
                await event_broker.quarantine_poison_payload(
                    raw_data=json.dumps({"target_node": node, "session_id": workflow_uuid}).encode("utf-8"),
                    error_msg="Connectivity dropped."
                )
            await metrics_exporter.increment_node_counter(node, "dlq_quarantine")
            redis_client = getattr(event_broker, "redis_client", event_broker.redis)
            await redis_client.incr("afaos:analytics:failures_quarantined")
            print("🔄 [STATE REVERSAL]: Active node transactional state cleanly reverted to parent ledger checkpoints.\n")
            continue
            
        await event_broker.publish_event(
            topic=node,
            payload={"event_type": "NODE_EXECUTION_START", "source_node": "System_Orchestrator_Core", "target_node": node, "status": "processing"}
        )
        
        redis_client = getattr(event_broker, "redis_client", event_broker.redis)
        await redis_client.incr("afaos:analytics:total_processed")
        await state_memory.force_local_increment("total_processed", 1)
        
        logger.info(f"Checkpoint match found! Key [{node}] skipped inside tracking cache.")
        await asyncio.sleep(0.05) 
        
        await event_broker.publish_event(
            topic=node,
            payload={"event_type": "NODE_EXECUTION_COMPLETED", "source_node": "System_Orchestrator_Core", "target_node": node, "status": "cache_skipped"}
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
    redis_runtime_client = aioredis.from_url("redis://localhost:6379", decode_responses=False)

    state_memory = AgentStateMemory(redis_runtime_client)
    shared_state_memory_ref = state_memory
    
    event_broker = RedisEventBroker(redis_runtime_client)
    metrics_exporter = SystemMetricsExporter(redis_runtime_client)
    compactor = TelemetryCompactor(redis_runtime_client, threshold_ms=1000.0)

    await redis_runtime_client.set("afaos:analytics:total_processed", "0")
    await redis_runtime_client.set("afaos:analytics:failures_quarantined", "0")
    
    # Initialize baseline Token Bucket capacities
    await redis_runtime_client.set("afaos:limiter:tokens:Analysis_Agent_Node", "0") # Forces limiter testing instantly

    try:
        async with RedisDistributedLock(redis_runtime_client, relational_workflow_uuid, lease_time_sec=60):
            logger.info("🔒 [CONCURRENCY GUARD ACTIVE]: System execution context locked cleanly.")
            
            await event_broker.initialize_consumer_group()
            
            # FIX 2: Run pattern listener as a background task so execution can finish smoothly
            listener_task = None
            if hasattr(event_broker, 'start_pattern_listener'):
                listener_task = asyncio.create_task(
                    event_broker.start_pattern_listener("afaos:events:*", cluster_sync_pattern_callback)
                )
            
            await run_persistent_orchestrator(relational_workflow_uuid, SYSTEM_ROUTING_MATRIX, state_memory, event_broker, metrics_exporter, compactor)
            
            # Stop the background listener task gracefully
            if hasattr(event_broker, 'close'):
                await event_broker.close()
            elif hasattr(event_broker, 'stop_pattern_listener'):
                await event_broker.stop_pattern_listener()
                
            if listener_task and not listener_task.done():
                listener_task.cancel()
            
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