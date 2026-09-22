import asyncio
import time
import json
import logging
import inspect

logger = logging.getLogger(__name__)

class SystemMetricsExporter:
    def __init__(self, redis_client):
        """Initializes the Day 21 Performance Exporter Engine using the active Redis connection."""
        self.redis_client = redis_client
        self.metrics_prefix = "afaos:metrics"
        self.latency_key = f"{self.metrics_prefix}:latency_ms"
        self.counter_key = f"{self.metrics_prefix}:node_counters"

    async def _safe_execute(self, target_callable, *args, **kwargs):
        """Helper to invoke a method and safely await it only if it returns a coroutine."""
        result = target_callable(*args, **kwargs)
        if asyncio.iscoroutine(result) or inspect.isawaitable(result):
            return await result
        return result

    async def record_node_latency(self, node_name: str, duration_ms: float):
        """Records the execution runtime duration of a node into a Redis Sorted Set."""
        try:
            member_id = f"{node_name}:{time.time()}"
            await self._safe_execute(self.redis_client.zadd, self.latency_key, {member_id: duration_ms})
            logger.info(f"📊 [METRIC EXPORT]: Latency for [{node_name}] recorded at {duration_ms:.2f} ms.")
        except Exception as err:
            logger.error(f"❌ [METRIC ERROR]: Failed to export latency metrics: {err}")

    async def increment_node_counter(self, node_name: str, metric_type: str):
        """Increments telemetry tracking counters inside the shared ledger engine."""
        try:
            field_name = f"{node_name}:{metric_type}"
            await self._safe_execute(self.redis_client.hincrby, self.counter_key, field_name, 1)
            logger.info(f"📈 [COUNTER INCREMENT]: Field '{field_name}' bumped cleanly.")
        except Exception as err:
            logger.error(f"❌ [METRIC ERROR]: Failed to increment counter states: {err}")

    async def fetch_live_dashboard_aggregations(self) -> dict:
        """Extracts operational counts to compile the high-frequency JSON snapshot dashboard."""
        try:
            raw_counters = await self._safe_execute(self.redis_client.hgetall, self.counter_key)
            
            counters = {}
            if raw_counters:
                for k, v in raw_counters.items():
                    key_str = k.decode('utf-8') if isinstance(k, bytes) else k
                    val_int = int(v.decode('utf-8')) if isinstance(v, bytes) else int(v)
                    counters[key_str] = val_int

            return {
                "system_status": "ONLINE",
                "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "telemetry_counters": counters
            }
        except Exception as err:
            logger.error(f"❌ [DASHBOARD ERROR]: Failed to compile telemetry metrics: {err}")
            return {"system_status": "DEGRADED", "error": str(err)}
