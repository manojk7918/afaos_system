import asyncio
import logging
from redis.asyncio import Redis

# Configure structured logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DistributedFaultRecoveryManager:
    def __init__(self, redis_url="redis://127.0.0.1:6379"):
        self.redis_url = redis_url
        self.redis = None
        self.stream_key = "afaos:stream:event_ledger"
        self.group_name = "afaos:group:orchestrator_workers"
        self.recovery_worker_name = "fault_recovery_daemon"

    async def initialize(self):
        """Establish high-concurrency asynchronous pool bindings."""
        self.redis = Redis.from_url(self.redis_url, decode_responses=True)
        logging.info("⚙️ Distributed Fault Recovery Engine initialized.")

    async def audit_and_recover_abandoned_tasks(self, idle_timeout_ms=5000):
        """
        Scans the Pending Entries List (PEL) for stale consumer processing items.
        Hijacks dead tasks using XCLAIM and re-routes them to the recovery node.
        """
        logging.info("🔍 Auditing stream processing ledger loops for abandoned tasks...")
        
        try:
            # 1. Fetch pending message summaries from the PEL
            pending_summary = await self.redis.xpending_range(
                self.stream_key, self.group_name, min="-", max="+", count=10
            )

            if not pending_summary:
                logging.info("✅ Active stream ledger is fully acknowledged. No stuck jobs discovered.")
                return

            for task in pending_summary:
                # Handle Dict type returned by modern redis-py versions
                if isinstance(task, dict):
                    msg_id = task.get("message_id")
                    consumer = task.get("consumer")
                    idle_time = task.get("idle_time") or task.get("idle") or 0
                    delivery_count = task.get("times_delivered") or task.get("otime") or 0
                
                # Handle List/Tuple type returned by alternative/legacy redis-py engines
                elif isinstance(task, (list, tuple)) and len(task) >= 4:
                    msg_id = task[0]
                    consumer = task[1]
                    idle_time = task[2]
                    delivery_count = task[3]
                else:
                    logging.warning(f"⚠️ Encountered unrecognized pending entry format: {task}")
                    continue

                logging.info(f"📋 Found pending task {msg_id} owned by worker '{consumer}' (Idle: {idle_time}ms, Tries: {delivery_count})")

                # 2. Check if the task has been abandoned past our threshold
                if idle_time > idle_timeout_ms:
                    logging.warning(f"⚠️ Task {msg_id} crossed safety threshold bounds! Initiating XCLAIM recovery...")
                    
                    # Hijack ownership of the message
                    claimed_messages = await self.redis.xclaim(
                        self.stream_key,
                        self.group_name,
                        self.recovery_worker_name,
                        min_idle_time=idle_timeout_ms,
                        keys=[msg_id]
                    )

                    if claimed_messages:
                        logging.info(f"🔒 [TASK HIJACK SUCCESSFUL]: Recovered task {msg_id}. Processing contents now...")
                        
                        # Process the recovered data payload safely
                        for claim_id, payload in claimed_messages:
                            logging.info(f"🛠️ Re-processing event body: {payload}")
                            
                            # Acknowledge completion to cleanly evict the task from the PEL matrix
                            await self.redis.xack(self.stream_key, self.group_name, claim_id)
                            logging.info(f"🏁 [RECOVERY COMPLETED]: Acknowledged and purged event {claim_id} from PEL.")
        
        except Exception as e:
            if "no such key" in str(e).lower():
                logging.info("ℹ️ Stream ledger is empty or consumer group has no processing frames yet.")
            else:
                logging.error(f"❌ Error encountered inside telemetry audit iteration: {e}")

    async def close(self):
        """Drop socket maps cleanly during lifecycle exit."""
        if self.redis:
            await self.redis.aclose()
            logging.info("🔒 Fault recovery engine connections closed safely.")

async def main():
    manager = DistributedFaultRecoveryManager()
    await manager.initialize()
    
    # Run an immediate evaluation run to scan for abandoned tasks
    try:
        await manager.audit_and_recover_abandoned_tasks(idle_timeout_ms=5000)
    finally:
        await manager.close()

if __name__ == "__main__":
    asyncio.run(main())
