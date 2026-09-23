import asyncio
import logging
import time
from redis.asyncio import Redis

# Configure structured logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class TelemetryMetricsExporter:
    def __init__(self, redis_url="redis://127.0.0.1:6379"):
        self.redis_url = redis_url
        self.redis = None
        self.stream_key = "afaos:stream:event_ledger"
        self.group_name = "afaos:group:orchestrator_workers"

    async def initialize(self):
        """Establish asynchronous connection pools to memory cache layer."""
        self.redis = Redis.from_url(self.redis_url, decode_responses=True)
        logging.info("⚙️ Telemetry Metrics Exporter Engine initialized.")

    async def generate_prometheus_metrics(self) -> str:
        """
        Scrapes cluster states and formats data into standard Prometheus time-series text notation.
        """
        metrics_output = []
        timestamp_ms = int(time.time() * 1000)

        try:
            # 1. Fetch current Redis Stream performance counters
            try:
                stream_info = await self.redis.xinfo_stream(self.stream_key)
                stream_len = stream_info.get("length", 0)
                consumer_count = stream_info.get("groups", 0)
            except Exception:
                stream_len = 0
                consumer_count = 0

            # 2. Fetch pending tasks from consumer groups
            try:
                pending_info = await self.redis.xpending(self.stream_key, self.group_name)
                pending_count = pending_info.get("pending", 0) if pending_info else 0
            except Exception:
                pending_count = 0

            # 3. Compile structural open metrics notation format entries
            metrics_output.append("# HELP afaos_stream_events_total Current total event count residing inside memory stream.")
            metrics_output.append("# TYPE afaos_stream_events_total gauge")
            metrics_output.append(f"afaos_stream_events_total{{stream=\"{self.stream_key}\"}} {stream_len} {timestamp_ms}")

            metrics_output.append("# HELP afaos_stream_pending_tasks_total Number of unacknowledged entries stalling in the PEL.")
            metrics_output.append("# TYPE afaos_stream_pending_tasks_total gauge")
            metrics_output.append(f"afaos_stream_pending_tasks_total{{group=\"{self.group_name}\"}} {pending_count} {timestamp_ms}")

            metrics_output.append("# HELP afaos_active_consumers_total Number of active microservice workers registered to consumer fleet.")
            metrics_output.append("# TYPE afaos_active_consumers_total gauge")
            metrics_output.append(f"afaos_active_consumers_total{{group=\"{self.group_name}\"}} {consumer_count} {timestamp_ms}")

        except Exception as e:
            logging.error(f"❌ Failed to parse telemetry matrix compilation: {e}")
            
        return "\n".join(metrics_output)

    async def close(self):
        """Clean up connection socket structures safely."""
        if self.redis:
            await self.redis.aclose()
            logging.info("🔒 Metrics Exporter connection pool closed cleanly.")

async def main():
    exporter = TelemetryMetricsExporter()
    await exporter.initialize()
    try:
        print("\n📊 --- SCRAPING LIVE TELEMETRY MATRIX SNAPSHOT ---")
        serialized_payload = await exporter.generate_prometheus_metrics()
        print(serialized_payload)
        print("📊 ------------------------------------------------\n")
    finally:
        await exporter.close()

if __name__ == "__main__":
    asyncio.run(main())
