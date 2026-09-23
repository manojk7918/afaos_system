import asyncio
import json
import logging
from datetime import datetime

logger = logging.getLogger("MetricsExporter")

class SystemMetricsExporter:
    def __init__(self, redis_client):
        """Initializes Day 21 Live Metrics Dashboard Engine."""
        self.redis_client = redis_client
        self.latency_prefix = "afaos:metrics:latency:"
        self.counter_prefix = "afaos:metrics:counters:"

    async def _safe_execute(self, target_callable, *args, **kwargs):
        """Internal helper to execute and await callables uniformly."""
        result = target_callable(*args, **kwargs)
        if asyncio.iscoroutine(result):
            return await result
        return result

    async def record_node_latency(self, node_key: str, duration_ms: float):
        """Appends raw processing durations to a Redis list for sliding window evaluation."""
        try:
            key = f"{self.latency_prefix}{node_key}"
            await self._safe_execute(self.redis_client.lpush, key, str(duration_ms))
            # Caps latency tracking history to the most recent 100 entries to save RAM
            await self._safe_execute(self.redis_client.ltrim, key, 0, 99)
        except Exception as err:
            logger.error(f"❌ [METRICS ERROR]: Failed to record node latency: {err}")

    async def increment_node_counter(self, node_key: str, metric_type: str):
        """Increments atomic metric tracking counters inside the cluster."""
        try:
            key = f"{self.counter_prefix}{node_key}:{metric_type}"
            await self._safe_execute(self.redis_client.incr, key)
        except Exception as err:
            logger.error(f"❌ [METRICS ERROR]: Failed to increment node counter: {err}")

    async def fetch_live_dashboard_aggregations(self) -> dict:
        """Compiles tracked performance variables into a clean JSON dashboard payload."""
        dashboard = {
            "exported_at": datetime.utcnow().isoformat() + "Z",
            "cluster_health": "OPTIMAL",
            "node_metrics": {}
        }
        
        nodes = ["Ingestion_Node", "Vector_Indexing_Node", "Analysis_Agent_Node", "Cloud_Dispatch_Node"]
        try:
            for node in nodes:
                success_key = f"{self.counter_prefix}{node}:execution_success"
                dlq_key = f"{self.counter_prefix}{node}:dlq_quarantine"
                latency_key = f"{self.latency_prefix}{node}"
                
                success_count = await self._safe_execute(self.redis_client.get, success_key)
                dlq_count = await self._safe_execute(self.redis_client.get, dlq_key)
                latencies = await self._safe_execute(self.redis_client.lrange, latency_key, 0, -1)
                
                # Normalize byte conversions safely if necessary
                def decode_val(v):
                    if v is None: return 0
                    return int(v.decode('utf-8') if isinstance(v, bytes) else v)
                
                float_latencies = [float(l.decode('utf-8') if isinstance(l, bytes) else l) for l in latencies if l]
                avg_latency = sum(float_latencies) / len(float_latencies) if float_latencies else 0.0
                
                dashboard["node_metrics"][node] = {
                    "execution_success_count": decode_val(success_count),
                    "dlq_quarantine_count": decode_val(dlq_count),
                    "average_latency_ms": round(avg_latency, 2)
                }
            return dashboard
        except Exception as err:
            logger.error(f"❌ [DASHBOARD ERROR]: Failed to aggregate live system telemetry: {err}")
            return {"error": str(err)}
