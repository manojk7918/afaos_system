import asyncio
import time
import logging
import inspect

logger = logging.getLogger(__name__)

class TelemetryCompactor:
    def __init__(self, redis_client, threshold_ms=1500.0):
        """
        Initializes the Day 22 Sliding Window Telemetry Analyzer and Log Compactor.
        """
        self.redis_client = redis_client
        self.latency_key = "afaos:metrics:latency_ms"
        self.archive_key = "afaos:metrics:latency_archive"
        self.threshold_ms = threshold_ms

    async def _safe_execute(self, target_callable, *args, **kwargs):
        """Helper to invoke a method and safely await it only if it returns a coroutine."""
        result = target_callable(*args, **kwargs)
        if asyncio.iscoroutine(result) or inspect.isawaitable(result):
            return await result
        return result

    async def analyze_sliding_window_anomalies(self, node_name: str):
        """
        Extracts recent sliding window metrics from the sorted set to detect execution spikes.
        """
        try:
            # Fetch all elements from the sorted set
            raw_elements = await self._safe_execute(self.redis_client.zrange, self.latency_key, 0, -1, withscores=True)
            
            latencies = []
            if raw_elements:
                for member, score in raw_elements:
                    member_str = member.decode('utf-8') if isinstance(member, bytes) else member
                    # Filter items belonging to this specific node
                    if member_str.startswith(f"{node_name}:"):
                        latencies.append(float(score))

            if not latencies:
                return

            # Compute moving average metric points
            moving_avg = sum(latencies) / len(latencies)
            latest_latency = latencies[-1]

            logger.info(f"🧐 [SLIDING WINDOW]: Node [{node_name}] moving baseline average: {moving_avg:.2f} ms | Current: {latest_latency:.2f} ms")

            # Anomaly circuit breaker evaluation check
            if latest_latency > self.threshold_ms:
                logger.warning(f"⚠️  [LATENCY ANOMALY ALERT]: Node [{node_name}] execution time spiked to {latest_latency:.2f} ms! (Threshold: {self.threshold_ms} ms)")
        except Exception as err:
            logger.error(f"❌ [ANOMALY ENGINE ERROR]: Failed to analyze sliding metrics window: {err}")

    async def compact_historical_logs(self, retention_seconds=10):
        """
        Sweeps raw historical telemetry timelines, saves them into summarized archive maps, 
        and prunes old high-frequency sorted set members to reclaim RAM footprint.
        """
        try:
            now = time.time()
            cutoff_time = now - retention_seconds
            raw_elements = await self._safe_execute(self.redis_client.zrange, self.latency_key, 0, -1, withscores=True)

            if not raw_elements:
                return

            compacted_counts = 0
            node_sums = {}
            node_counts = {}

            # Identify entries past our active retention boundary window
            for member, score in raw_elements:
                member_str = member.decode('utf-8') if isinstance(member, bytes) else member
                try:
                    node_name, timestamp_str = member_str.split(":")
                    timestamp = float(timestamp_str)
                    
                    if timestamp < cutoff_time:
                        node_sums[node_name] = node_sums.get(node_name, 0.0) + float(score)
                        node_counts[node_name] = node_counts.get(node_name, 0) + 1
                        
                        # Remove the high-frequency member out of the live sorted set layer
                        await self._safe_execute(self.redis_client.zrem, self.latency_key, member)
                        compacted_counts += 1
                except ValueError:
                    continue

            # If expired records were cleared, save them as compressed history baselines
            if compacted_counts > 0:
                for node_name in node_sums:
                    avg_historical_latency = node_sums[node_name] / node_counts[node_name]
                    await self._safe_execute(self.redis_client.hset, self.archive_key, f"{node_name}:historical_avg_ms", f"{avg_historical_latency:.2f}")
                
                logger.info(f"🧹 [COMPACTION COMPLETE]: Cleaned up {compacted_counts} high-frequency records. Historical averages archived.")
        except Exception as err:
            logger.error(f"❌ [COMPACTOR ERROR]: Failed to run background log retention compaction: {err}")
