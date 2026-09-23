import asyncio
import json
import logging
from datetime import datetime

logger = logging.getLogger("StateMemory")

class AgentStateMemory:
    def __init__(self, redis_client=None):
        """Initializes Day 27 Split-State Cache Memory with Multi-Node Sync capabilities."""
        if redis_client:
            self.client = redis_client
        else:
            import redis.asyncio as aioredis
            self.client = aioredis.from_url("redis://localhost:6379", decode_responses=True)
            
        self.registry_sync_key = "afaos:registry:master_state"
        self._local_metrics_cache = {
            "total_processed": 0,
            "failures_quarantined": 0
        }

    async def get_local_metric(self, metric_key: str) -> int:
        """Safely extracts a tracked local in-memory node counter value."""
        return self._local_metrics_cache.get(metric_key, 0)

    async def force_local_increment(self, metric_key: str, increment_value: int = 1):
        """Simulates atomic workloads advancing in-memory statistics before background syncs fire."""
        if metric_key in self._local_metrics_cache:
            self._local_metrics_cache[metric_key] += increment_value

    async def generate_master_snapshot(self) -> dict:
        """
        Compiles high-performance state fields from the primary cache into an
        authoritative transaction blueprint to broadcast out onto the cluster network.
        """
        try:
            redis_total = await self.client.get("afaos:analytics:total_processed")
            redis_fail = await self.client.get("afaos:analytics:failures_quarantined")
            
            snapshot = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "master_registry": {
                    "total_processed": int(redis_total) if redis_total else 0,
                    "failures_quarantined": int(redis_fail) if redis_fail else 0
                }
            }
            await self.client.set(self.registry_sync_key, json.dumps(snapshot))
            return snapshot
        except Exception as err:
            logger.error(f"❌ [SNAPSHOT FAILED]: Failed to compile cluster state snapshot: {err}")
            return {"master_registry": self._local_metrics_cache}

    async def evaluate_and_reconcile_drift(self, cluster_snapshot_payload: dict) -> bool:
        """
        Day 27 Core Logic: Compares local worker metrics directly against the master matrix payload.
        Triggers a hot catch-up loop if a distributed drift anomaly is detected.
        """
        master_data = cluster_snapshot_payload.get("master_registry", {})
        drift_detected = False
        
        for key in self._local_metrics_cache.keys():
            master_val = master_data.get(key, 0)
            local_val = self._local_metrics_cache[key]
            
            if local_val != master_val:
                logger.warning(
                    f"⚠️ [STATE DRIFT DETECTED]: Cluster variation identified on metric '{key}'! "
                    f"Master Registry: {master_val} | Local Worker Cache: {local_val}"
                )
                drift_detected = True
        
        if drift_detected:
            logger.info("🔄 [RECONCILIATION INITIALIZED]: Performing hot local catch-up sync with master registry...")
            for key in self._local_metrics_cache.keys():
                self._local_metrics_cache[key] = master_data.get(key, 0)
            logger.info("✅ [SYNCHRONIZATION COMPLETE]: Local memory matrices aligned with cluster state.")
            return True
            
        logger.info("🍏 [STATE ALIGNED]: Local worker states match the master cluster blueprint perfectly.")
        return False
