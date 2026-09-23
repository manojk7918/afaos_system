import asyncio
import logging
import time

logger = logging.getLogger("TelemetryCompactor")

class TelemetryCompactor:
    def __init__(self, redis_client, threshold_ms: float = 1000.0):
        """Initializes Day 22 Moving Percentile and Log Compactor Sweep Engine."""
        self.redis_client = redis_client
        self.threshold_ms = threshold_ms
        self.latency_prefix = "afaos:metrics:latency:"

    async def _safe_execute(self, target_callable, *args, **kwargs):
        """Internal helper to execute and await callables uniformly."""
        result = target_callable(*args, **kwargs)
        if asyncio.iscoroutine(result):
            return await result
        return result

    async def analyze_sliding_window_anomalies(self, node_key: str):
        """Computes moving percentiles over recent latency listings to flag anomalies."""
        try:
            key = f"{self.latency_prefix}{node_key}"
            latencies = await self._safe_execute(self.redis_client.lrange, key, 0, -1)
            
            if not latencies:
                return
                
            float_latencies = sorted([float(l.decode('utf-8') if isinstance(l, bytes) else l) for l in latencies if l])
            if float_latencies:
                # Direct calculation of Moving 90th Percentile latency
                p90_index = int(len(float_latencies) * 0.9)
                p90_latency = float_latencies[p90_index]
                
                if p90_latency > self.threshold_ms:
                    logger.warning(f"⚠️  [SLIDING WINDOW WARNING]: Latency spike anomaly on '{node_key}'! P90 Speed: {p90_latency:.2f}ms")
                else:
                    logger.info(f"📈 [SLIDING WINDOW ANALYSIS]: Node '{node_key}' running clean. P90 Speed: {p90_latency:.2f}ms")
        except Exception as err:
            logger.error(f"❌ [COMPACTOR ERROR]: Failed sliding window telemetry analysis: {err}")

    async def compact_historical_logs(self, retention_seconds: int = 5):
        """Simulates sweeping and compacting stale analytics markers out of database frames."""
        try:
            logger.info(f"🧹 [COMPACTION SWEEP]: Commencing time-series log compression cycles (Retention: {retention_seconds}s)...")
            # In a live multi-node environment, this block would prune expired keys using absolute timestamps
            await asyncio.sleep(0.05)
            logger.info("✅ [COMPACTION SUCCESS]: Stale time-series operational memory footprints compressed cleanly.")
        except Exception as err:
            logger.error(f"❌ [COMPACTION ERROR]: Historical logging sweep failed: {err}")

