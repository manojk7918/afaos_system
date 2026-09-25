import os
import asyncio
import logging
import uuid
import sqlite3
from redis.asyncio import Redis
from rate_limiter import DistributedRateLimiter  # Import our Day 33 gatekeeper

# Configure structured logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class IntegratedOrchestrator:
    def __init__(self, instance_id=None, redis_url=None, db_path=None):
        self.instance_id = instance_id or str(uuid.uuid4())
        
        # Read environment variables set by Docker-Compose, fallback to localhost for bare-metal
        redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
        redis_port = os.getenv("REDIS_PORT", "6379")
        
        self.redis_url = redis_url or f"redis://{redis_host}:{redis_port}"
        self.db_path = db_path or os.getenv("DB_PATH", "afaos_audit.db")
        self.lock_key = "lock:workflow:fa15b023-5e8c-411a-bd63-902fd7b8e1a4"
        
        # Core layers
        self.redis = None
        self.rate_limiter = None
        self.db_conn = None

    async def initialize_system(self):
        """Bootstrap database tables, Redis connections, and distributed rate-limiter."""
        logging.info(f"Initializing permanent relational storage engine layer: {self.db_path}")
        self.db_conn = sqlite3.connect(self.db_path)
        cursor = self.db_conn.cursor()
        
        # Match your exact production schema discovered via PRAGMA table_info
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS execution_audit_logs (
                id TEXT PRIMARY KEY,
                workflow_id TEXT,
                node_key TEXT,
                assigned_agent TEXT,
                completion_status TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                instance_id TEXT
            )
        """)
        self.db_conn.commit()

        logging.info(f"Initializing high-concurrency Asynchronous Redis Connection Pools on {self.redis_url}...")
        self.redis = Redis.from_url(self.redis_url, decode_responses=True)
        
        # Initialize the gatekeeper companion module
        self.rate_limiter = DistributedRateLimiter(redis_url=self.redis_url)
        await self.rate_limiter.initialize()

    async def acquire_distributed_lock(self) -> bool:
        """Secure single-instance distributed lock execution safety."""
        is_locked = await self.redis.set(self.lock_key, self.instance_id, ex=60, nx=True)
        if is_locked:
            logging.info(f"🔒 [LOCK ACQUIRED]: Successfully locked key '{self.lock_key}' for instance {self.instance_id}")
            logging.info("🔒 [CONCURRENCY GUARD ACTIVE]: System execution context locked cleanly.")
            return True
        return False

    async def process_transaction_event(self, event_id: str, payload: dict):
        """Simulate passing the financial ledger pipeline event down onto SQLite disk storage."""
        try:
            cursor = self.db_conn.cursor()
            
            # Map incoming Redis stream fields to your exact SQLite schema columns
            workflow_id = payload.get("reference_id", f"WF-{event_id}")
            node_key = payload.get("type", "unknown_txn")
            assigned_agent = f"{node_key}_agent_node"
            completion_status = "COMMITTED"

            cursor.execute(
                """
                INSERT INTO execution_audit_logs 
                (id, workflow_id, node_key, assigned_agent, completion_status, instance_id) 
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (event_id, workflow_id, node_key, assigned_agent, completion_status, self.instance_id)
            )
            self.db_conn.commit()
            logging.info(f"💾 [SQLITE PERSISTED]: Event {event_id} committed securely to disk archive.")
        except Exception as e:
            logging.error(f"❌ Failed to archive transaction log down to disk: {e}")

    async def run_event_listener_loop(self):
        """Continuously polls for incoming events while routing through admission gatekeeper."""
        group_name = "afaos:group:orchestrator_workers"
        stream_key = "afaos:stream:event_ledger"
        
        # Ensure Stream and Consumer Group frameworks are active
        try:
            await self.redis.xgroup_create(stream_key, group_name, id="0", mkstream=True)
        except Exception:
            logging.info(f"👥 [CONSUMER GROUP ACTIVE]: Group '{group_name}' already exists.")

        logging.info("Subscribed to wildcard pattern: afaos:events:*")
        print("\n🚀 Orchestrator active and parsing transactions. Press [Ctrl + C] to terminate.")

        try:
            while True:
                # Read stream payloads as a consumer worker node
                # Using short polling blocks to remain reactive yet gentle on CPU cycles
                response = await self.redis.xreadgroup(group_name, self.instance_id, {stream_key: ">"}, count=1, block=1000)
                
                if response:
                    for stream, messages in response:
                        for msg_id, payload in messages:
                            logging.info(f"📥 Inbound Stream event intercepted: {msg_id}")
                            
                            # Apply Day 34 Integrated Admission Gatekeeper Control
                            # Set aggressive limits (max 3 requests per 10 seconds per node) for protection
                            allowed = await self.rate_limiter.is_allowed(client_id=self.instance_id, max_requests=3, window_seconds=10)
                            
                            if allowed:
                                await self.process_transaction_event(msg_id, payload)
                                # Acknowledge task removal from PEL matrix
                                await self.redis.xack(stream_key, group_name, msg_id)
                            else:
                                logging.warning(f"🛑 [BACKOFF PACING]: Dropping processing pipeline window for task {msg_id} due to traffic limits.")
                
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            pass

    async def close(self):
        """Cleanly close all communication pipes and databases."""
        if self.redis:
            # Release lock context safely before leaving
            current_lock_owner = await self.redis.get(self.lock_key)
            if current_lock_owner == self.instance_id:
                await self.redis.delete(self.lock_key)
                logging.info("🔒 [LOCK RELEASED]: System lock key dropped cleanly during shutdown.")
            await self.redis.aclose()
        if self.rate_limiter:
            await self.rate_limiter.close()
        if self.db_conn:
            self.db_conn.close()
        logging.info("🔒 System connections shut down gracefully.")

async def main():
    orchestrator = IntegratedOrchestrator()
    await orchestrator.initialize_system()
    
    if not await orchestrator.acquire_distributed_lock():
        logging.error("❌ Concurrency lock execution conflict! Another orchestrator instance is running. Aborting.")
        if orchestrator.db_conn:
            orchestrator.db_conn.close()
        return

    try:
        await orchestrator.run_event_listener_loop()
    except KeyboardInterrupt:
        print("\n👋 Shutdown signal detected via manual keyboard interrupt.")
    finally:
        await orchestrator.close()

if __name__ == "__main__":
    asyncio.run(main())