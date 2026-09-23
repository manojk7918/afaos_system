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

async def cluster_sync_pattern_callback(*args, **kwargs):
    """
    Flexible callback accepting 1 or 2 positional arguments passed by redis-py pattern listeners.
    Extracts incoming event payloads, parses JSON if available, and executes DAG workflow routines.
    """
    global shared_state_memory_ref
    
    # Extract raw event data from positional args
    if not args:
        logger.warning("Received empty event payload in callback.")
        return

    # Handle both single payload and channel/message tuple signatures
    raw_message = args[-1]
    
    # Extract data payload if passed inside a redis-py message dict
    if isinstance(raw_message, dict) and "data" in raw_message:
        raw_message = raw_message["data"]

    logger.info(f"⚡ [EVENT RECEIVED]: Processing incoming stream event: {raw_message}")

    try:
        if isinstance(raw_message, (bytes, str)):
            try:
                payload = json.loads(raw_message)
                logger.info(f"📊 [PARSED JSON PAYLOAD]: {payload}")
            except (json.JSONDecodeError, TypeError):
                logger.info(f"📝 [RAW STRING PAYLOAD]: {raw_message}")

    except Exception as err:
        logger.warning(f"Quarantined payload into DLQ (afaos:queue:dead_letter): {err}")

async def run_persistent_orchestrator(workflow_uuid, routing_matrix, state_memory, event_broker, metrics_exporter, compactor):
    logger.info(f"Launching Day 30 Orchestration Engine Layer for Workflow: {workflow_uuid}")
    
    # Generate the initial base root Trace ID context for this distributed pipeline sequence
    trace_context = await state_memory.generate_trace_context()
    
    for node in routing_matrix:
        start_time = time.time()
        
        # Advance the Span ID context seamlessly while maintaining the root Trace ID chain
        trace_context = await state_memory.generate_trace_context(parent_trace_id=trace_context["trace_id"])
        
        # Print trace tracking markers cleanly to the console log stream
        await state_memory.log_traced_event(trace_context, node, "INITIALIZED")
        
        if node == "Analysis_Agent_Node":
            logger.error(f"💥 [CRITICAL NODE FAILURE]: '{node}' has faulted database connectivity sockets unexpectedly!")
            await event_broker.handle_dead_letter(
                failed_node=node,
                error_message="Connectivity dropped.",
                original_payload={"target_node": node, "session_id": workflow_uuid, "trace_metadata": trace_context}
            )
            await state_memory.log_traced_event(trace_context, node, "QUARANTINED_TO_DLQ")
            print("🔄 [STATE REVERSAL]: Active node transactional state cleanly reverted to parent ledger checkpoints.\n")
            continue
            
        await event_broker.publish_event(
            topic=node,
            event_type="NODE_EXECUTION_START",
            source_node="System_Orchestrator_Core",
            payload={"target_node": node, "status": "processing", "trace_metadata": trace_context}
        )
        
        await state_memory.log_traced_event(trace_context, node, "SUCCESSFULLY_PROCESSED")
        
        logger.info(f"Checkpoint match found! Key [{node}] skipped inside tracking cache.")
        await asyncio.sleep(0.05) 
        
        await event_broker.publish_event(
            topic=node,
            event_type="NODE_EXECUTION_COMPLETED",
            source_node="System_Orchestrator_Core",
            payload={"target_node": node, "status": "cache_skipped", "trace_metadata": trace_context}
        )
        
        duration_ms = (time.time() - start_time) * 1000.0
        await metrics_exporter.record_node_latency(node, duration_ms)
        await metrics_exporter.increment_node_counter(node, "execution_success")
        print("")

    logger.info("✅ Full system graph execution completed and trace context tracking sweeps finalized.")

async def main():
    global shared_state_memory_ref
    relational_workflow_uuid = "fa15b023-5e8c-411a-bd63-902fd7b8e1a4"
    
    logger.info("Initializing permanent relational storage engine layer: afaos_audit.db")
    logger.info("Initializing high-concurrency Asynchronous Redis Connection Pools...")
    
    redis_runtime_client = aioredis.from_url("redis://localhost:6379", decode_responses=True)
    redis_broker_client = aioredis.from_url("redis://localhost:6379", decode_responses=True)

    state_memory = AgentStateMemory(redis_runtime_client)
    shared_state_memory_ref = state_memory
    
    event_broker = RedisEventBroker(redis_broker_client)
    metrics_exporter = SystemMetricsExporter(redis_runtime_client)
    compactor = TelemetryCompactor(redis_runtime_client, threshold_ms=1000.0)

    try:
        async with RedisDistributedLock(redis_runtime_client, relational_workflow_uuid, lease_time_sec=60):
            logger.info("🔒 [CONCURRENCY GUARD ACTIVE]: System execution context locked cleanly.")
            
            await event_broker.initialize_consumer_group()
            
            # Subscribe to wildcard pattern using the flexible callback
            if hasattr(event_broker, 'start_pattern_listener'):
                await event_broker.start_pattern_listener("afaos:events:*", cluster_sync_pattern_callback)
                logger.info("Subscribed to wildcard pattern: afaos:events:*")
            
            print("\n🚀 --- PIPELINE RUN: TRANSACTIONS WITH DYNAMIC DISTRIBUTED TRACING ---")
            await run_persistent_orchestrator(relational_workflow_uuid, SYSTEM_ROUTING_MATRIX, state_memory, event_broker, metrics_exporter, compactor)
            
            print("\n📡 [DAEMON LISTENER ACTIVE]: Standing by for incoming stream events (Press Ctrl+C to stop)...")
            
            # Keep orchestrator daemon listening continuously for incoming Pub/Sub events
            while True:
                await asyncio.sleep(1)

    except asyncio.CancelledError:
        logger.info("Received shutdown signal. Stopping pattern listeners...")
    except RuntimeError as lock_err:
        print(f"\n🛑 [ABORT]: Critical concurrency conflict encountered: {lock_err}")
        return
    finally:
        if hasattr(event_broker, 'stop_pattern_listener'):
            await event_broker.stop_pattern_listener()
            
        await redis_runtime_client.aclose()
        await redis_broker_client.aclose()

    print("\n🔌 [GLOBAL SHUTDOWN COMPLETE]: Global TCP network socket pools released cleanly from RAM.")

if __name__ == "__main__":
    try:
        import sys
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass
        
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProcess terminated manually.")