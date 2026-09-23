import asyncio
import logging
import sqlite3
import os
from redis.asyncio import Redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class TelemetryDashboard:
    def __init__(self, redis_url="redis://127.0.0.1:6379", db_path="afaos_audit.db"):
        self.redis_url = redis_url
        self.db_path = db_path
        self.redis = None

    async def initialize(self):
        """Establish high-performance asynchronous connection layers."""
        self.redis = Redis.from_url(self.redis_url, decode_responses=True)

    async def fetch_metrics(self, stream_key="afaos:stream:event_ledger"):
        """Collect systemic health markers from memory and disk structures."""
        metrics = {
            "redis_stream_len": 0,
            "sqlite_log_count": 0,
            "system_status": "HEALTHY"
        }
        
        # 1. Inspect Redis live memory footprint
        try:
            stream_info = await self.redis.xinfo_stream(stream_key)
            metrics["redis_stream_len"] = stream_info.get("length", 0)
        except Exception as e:
            if "no such key" in str(e).lower():
                metrics["redis_stream_len"] = 0
            else:
                metrics["system_status"] = "DEGRADED"

        # 2. Inspect SQLite permanent disk footprint
        try:
            if os.path.exists(self.db_path):
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='execution_audit_logs'")
                if cursor.fetchone():
                    cursor.execute("SELECT COUNT(*) FROM execution_audit_logs")
                    metrics["sqlite_log_count"] = cursor.fetchone()[0]
                conn.close()
        except Exception:
            metrics["system_status"] = "DEGRADED"

        return metrics

    def render_view(self, metrics):
        """Render a clean, structured operational dashboard view directly to standard output."""
        # Clear screen for terminal animation effect
        os.system('cls' if os.name == 'nt' else 'clear')
        
        print("=" * 60)
        print(" 🖥️  AFAOS SYSTEM REAL-TIME OPERATIONAL TELEMETRY DASHBOARD")
        print("=" * 60)
        print(f" 🟢 System Status         : {metrics['system_status']}")
        print(f" 💾 Redis Stream Memory   : {metrics['redis_stream_len']} live events")
        print(f" 📊 SQLite Permanent Disk : {metrics['sqlite_log_count']} logs committed")
        print("-" * 60)
        print(" Monitoring active loops... Press [Ctrl + C] to terminate dashboard.")
        print("=" * 60)

    async def start_monitoring_loop(self, interval=3):
        """Execute continuous pooling of health matrices."""
        try:
            while True:
                metrics = await self.fetch_metrics()
                self.render_view(metrics)
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            pass

    async def close(self):
        if self.redis:
            await self.redis.aclose()
            logging.info("\n🔒 Dashboard diagnostic interface closed cleanly.")

async def main():
    dashboard = TelemetryDashboard()
    await dashboard.initialize()
    try:
        await dashboard.start_monitoring_loop()
    except KeyboardInterrupt:
        pass
    finally:
        await dashboard.close()

if __name__ == "__main__":
    asyncio.run(main())
