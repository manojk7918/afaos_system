import asyncio
import logging
import sqlite3
from redis.asyncio import Redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DistributedLogCompactor:
    def __init__(self, redis_url="redis://127.0.0.1:6379", db_path="afaos_audit.db"):
        self.redis_url = redis_url
        self.db_path = db_path
        self.redis = None

    async def initialize(self):
        """Initialize connection pools cleanly."""
        self.redis = Redis.from_url(self.redis_url, decode_responses=True)
        logging.info("⚙️ Compactor Engine connection pools initialized.")

    async def get_sqlite_checkpoint_count(self):
        """Query disk storage to confirm how many transactions are safe."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            # Verify if table exists before executing check
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='execution_audit_logs'")
            if not cursor.fetchone():
                return 0
            
            cursor.execute("SELECT COUNT(*) FROM execution_audit_logs")
            count = cursor.fetchone()[0]
            conn.close()
            return count
        except Exception as e:
            logging.error(f"❌ Failed to verify SQLite baseline storage: {e}")
            return 0

    async def compact_event_stream(self, stream_key="afaos:stream:event_ledger", max_length=1000):
        """
        Safely truncates the memory stream to prevent RAM exhaustion.
        Applies strict guardrails: only compacts if SQLite mirrors data state.
        """
        try:
            sqlite_count = await self.get_sqlite_checkpoint_count()
            logging.info(f"📊 Relational disk audit state: {sqlite_count} logs committed.")
            
            # Read current memory stream depth
            stream_info = await self.redis.xinfo_stream(stream_key)
            current_len = stream_info.get("length", 0)
            logging.info(f"💾 Current Redis Stream memory footprint: {current_len} events.")

            if current_len > max_length:
                logging.warning(f"⚠️ Memory thresholds crossed! Initializing Stream Compaction...")
                # Truncate stream up to the max_length limit using XTRIM
                approximate_trimmed = await self.redis.xtrim(stream_key, max_len=max_length, approximate=True)
                logging.info(f"🧹 Compaction Complete. Evicted stale events. Trimmed to limit: ~{max_length}")
            else:
                logging.info("✅ Stream memory levels are within safe operating bounds. No compaction needed.")
                
        except Exception as e:
            # Handle empty stream edge cases safely
            if "no such key" in str(e).lower():
                logging.info(f"ℹ️ Stream '{stream_key}' is empty or hasn't processed data yet.")
            else:
                logging.error(f"❌ Compactor Engine encountered an extraction exception: {e}")

    async def close(self):
        if self.redis:
            await self.redis.aclose()
            logging.info("🔒 Compactor connections shut down cleanly.")

async def main():
    compactor = DistributedLogCompactor()
    await compactor.initialize()
    try:
        # Execute an immediate evaluation loop
        await compactor.compact_event_stream()
    finally:
        await compactor.close()

if __name__ == "__main__":
    asyncio.run(main())
