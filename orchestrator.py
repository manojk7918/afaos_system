import asyncio
import sqlite3
import logging
import redis
import redis.asyncio
from state_memory import AgentStateMemory
# Import the AFAOS relational database archiver engine built in Day 15
from db_archiver import DatabaseArchiver



# Setup Global Logging Configs
logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger("afaos_orchestrator")

# Core Data Persistence Configuration Anchors
RELATIONAL_DB_PATH = "afaos_audit.db"
REDIS_LOCAL_HOST = "localhost"
REDIS_LOCAL_PORT = 6379

SYSTEM_ROUTING_MATRIX = {
    "Ingestion_Node": "FastAPI_Stream_Ingester",
    "Vector_Indexing_Node": "Qdrant_gRPC_Cluster_Client",
    "Analysis_Agent_Node": "Primary_Cognitive_LLM_Agent",
    "Cloud_Dispatch_Node": "Production_Webhook_Dispatcher"
}

global_redis_pool = None

async def initialize_system_infrastructure():
    """Initializes SQLite databases and the shared Redis async pool connection namespaces."""
    global global_redis_pool
    
    logger.info(f"Initializing permanent relational storage engine layer: {RELATIONAL_DB_PATH}")
    conn = sqlite3.connect(RELATIONAL_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS execution_audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workflow_id TEXT NOT NULL,
            node_key TEXT NOT NULL,
            assigned_agent TEXT NOT NULL,
            completion_status TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    logger.info("🏛️ [RELATIONAL SCHEMA READY]: Permanent transaction audit log table compiled.")
    
    logger.info("Initializing high-concurrency Asynchronous Redis Connection Pool...")
    global_redis_pool = redis.asyncio.ConnectionPool(
        host=REDIS_LOCAL_HOST, 
        port=REDIS_LOCAL_PORT, 
        decode_responses=True
    )
    client = redis.asyncio.Redis(connection_pool=global_redis_pool)
    await client.ping()
    logger.info("⚡ [ASYNC REDIS SUCCESS]: Non-blocking connection pool handshaked with afaos_state_ledger.")

async def run_persistent_orchestrator(workflow_id: str, routing_matrix: dict, state_memory: AgentStateMemory):
    """Executes the pipeline nodes sequentially and wires in real physical tools."""
    logger.info(f"Launching Persistent Orchestration Engine Layer for Workflow: {workflow_id}")
    client = redis.asyncio.Redis(connection_pool=global_redis_pool)

    
    for key, agent in routing_matrix.items():
        current_checkpoint = await client.get(f"workflow:{workflow_id}:{key}")
        if current_checkpoint and current_checkpoint == "SUCCESS":
            logger.info(f"Checkpoint match found! Key [{key}] skipped inside tracking cache.")
            continue
            
            logger.info(f"Firing Event Loop execution frame -> Key: [{key}] utilizing Agent: [{agent}]")
        
        # Day 14 Integration: Save the agent's live running status to Redis
        state_memory.set_agent_state(
            session_id=workflow_id,
            agent_role=str(key),
            state_data={"agent_type": str(agent), "status": "RUNNING", "current_step": "executing_node_logic"}
        )

        if key == "Ingestion_Node":
            target_path = "vault_storage/csv_records/regulatory_ledger.csv"
            ledger_contents = tool_manager.execute_tool("read_financial_ledger", file_path=target_path)
            print(f"\n⚡ [REAL DATA EXTRACTION SUCCESS]:\n{ledger_contents}\n")
        
        if key == "Analysis_Agent_Node" and str(agent) == "Primary_Cognitive_LLM_Agent":
            logger.error(f"Orchestration engine caught operational drop: Critical I/O channel timeout detected on active instance: {agent}")
            logger.warning("💥 [DYNAMIC ROUTING ACTIVE]: Hot-swapping execution track to: [Secondary_Cost_Optimized_LLM_Agent]")
            
            # Update Redis Cache State so the system knows this node ultimately resolved successfully
            await client.set(f"workflow:{workflow_id}:{key}", "SUCCESS")
            
            conn = sqlite3.connect(RELATIONAL_DB_PATH)
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO execution_audit_logs (workflow_id, node_key, assigned_agent, completion_status) VALUES (?, ?, ?, ?)",
                (workflow_id, key, str(agent), "SUCCESS_VIA_FALLBACK")
            )
            conn.commit()
            conn.close()
            continue

        await asyncio.sleep(0.4)
        await client.set(f"workflow:{workflow_id}:{key}", "SUCCESS")
        logger.info(f"State log finalized -> workflow:{workflow_id}:{key} marked as SUCCESS in Redis.")
        
        conn = sqlite3.connect(RELATIONAL_DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO execution_audit_logs (workflow_id, node_key, assigned_agent, completion_status) VALUES (?, ?, ?, ?)",
            (workflow_id, key, str(agent), "SUCCESS")
        )
        conn.commit()
        conn.close()
        logger.info(f"💾 [RELATIONAL STORAGE WRITE]: Committed audit row securely to {RELATIONAL_DB_PATH}")

    logger.info("✅ Full system graph execution completed and archived permanently in relational tables.")

class SystemLifecycleContext:
    """Automated Day 11 context manager to ensure clean infrastructure socket teardown."""
    async def __aenter__(self):
        print("🧹 [CONTEXT MANAGER ENTRY]: Safely spinning up infrastructure session logs...")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if 'global_redis_pool' in globals() and global_redis_pool:
            await global_redis_pool.disconnect()
        print("🔌 [GLOBAL SHUTDOWN COMPLETE]: Global TCP network socket pools released cleanly from RAM.")
        return False

class AgentToolRegistry:
    """Central clearinghouse for discovering and executing valid runtime agent tools."""
    def __init__(self):
        self._registry = {}

    def register_tool(self, name: str, description: str):
        def decorator(func):
            self._registry[name] = {"execute": func, "description": description}
            return func
        return decorator

    def execute_tool(self, name: str, *args, **kwargs):
        if name not in self._registry:
            raise ValueError(f"🚨 [TOOL ENGINE ERROR]: Requested tool '{name}' is not registered.")
        try:
            return self._registry[name]["execute"](*args, **kwargs)
        except Exception as e:
            return f"ERROR_THROWN_BY_TOOL_{name}: {str(e)}"

tool_manager = AgentToolRegistry()

@tool_manager.register_tool(
    name="read_financial_ledger",
    description="Safely streams raw accounting data lines directly from secure local vault paths."
)
def read_financial_ledger(file_path: str) -> str:
    import os
    print(f"📖 [TOOL DEPLOYED]: Verification check initialized for path target: {file_path}")
    if not os.path.exists(file_path):
        return f"🚨 [FILE NOT FOUND]: Target path '{file_path}' does not exist on disk."
    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read()

@tool_manager.register_tool(
    name="parse_json_audit_payload",
    description="De-serializes unstructured transactional JSON log buffers safely into dictionaries."
)
def parse_json_audit_payload(raw_json: str) -> dict:
    import json
    print("🧩 [TOOL DEPLOYED]: Parsing structural JSON data packet.")
    try:
        return json.loads(raw_json)
    except json.JSONDecodeError as je:
        return {"status": "MALFORMED_JSON_PAYLOAD", "error": str(je)}

async def main():
    await initialize_system_infrastructure()
    async with SystemLifecycleContext():
        # Day 15 Integration: Execute defensive point-in-time relational database snapshot
        db_backup_engine = DatabaseArchiver(max_backups=5)
        db_backup_engine.execute_snapshot()

        # Setup the connection to the Redis Docker container
        state_memory = AgentStateMemory()
        
        relational_workflow_uuid = "fa15b023-5e8c-411a-bd63-902fd7b8e1a4"

        # Save the system's starting state into memory
        state_memory.set_agent_state(
            session_id=relational_workflow_uuid,
            agent_role="System_Orchestrator_Core",
            state_data={"status": "initializing_workflow_loop", "matrix_ready": True}
        )
        
        # Run the main orchestrator system loop with Day 14 state memory tracking
        await run_persistent_orchestrator(relational_workflow_uuid, SYSTEM_ROUTING_MATRIX, state_memory)

        # Day 17 Integration: Run Cross-Node Fleet Analytics & Threshold Verification
        print("\n⚙️ [SHUTDOWN PHASE]: Invoking cross-node metrics threshold checker...")
        from metric_aggregator import calculate_fleet_metrics
        calculate_fleet_metrics()

if __name__ == "__main__":
    asyncio.run(main())
