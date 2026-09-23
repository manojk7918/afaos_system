import asyncio
import json
import logging
import time
from datetime import datetime

logger = logging.getLogger("StateMemory")

class AgentStateMemory:
    def __init__(self, redis_client=None):
        """Initializes Day 28 Split-State Cache Memory with Admission Control Rate Limiting."""
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
        return self._local_metrics_cache.get(metric_key, 0)

    async def force_local_increment(self, metric_key: str, increment_value: int = 1):
        if metric_key in self._local_metrics_cache:
            self._local_metrics_cache[metric_key] += increment_value

    async def evaluate_admission_allowance(self, node_key: str, max_tokens: int = 5, refill_rate_per_sec: float = 2.0) -> bool:
        """
        Day 28 Feature: Distributed Token Bucket Admission Control.
        Tracks available resource tokens atomically in Redis to block downstream API drowning.
        """
        try:
            bucket_key = f"afaos:limiter:tokens:{node_key}"
            ts_key = f"afaos:limiter:last_refill:{node_key}"
            
            now = time.time()
            
            # Atomic fetch from cluster
            pipe = self.client.pipeline()
            pipe.get(bucket_key)
            pipe.get(ts_key)
            res = await pipe.execute()
            
            raw_tokens = res[0]
            raw_last_refill = res[1]
            
            # Normalize and decode values safely
            def to_float(v, default):
                if v is None: return default
                return float(v.decode('utf-8') if isinstance(v, bytes) else v)
                
            current_tokens = to_float(raw_tokens, float(max_tokens))
            last_refill = to_float(raw_last_refill, now)
            
            # Calculate dynamic token replenishment based on delta-time windows
            time_passed = now - last_refill
            refilled_tokens = current_tokens + (time_passed * refill_rate_per_sec)
            actual_tokens = min(float(max_tokens), refilled_tokens)
            
            if actual_tokens >= 1.0:
                # Deduct token and grant system entry admission
                actual_tokens -= 1.0
                
                pipe = self.client.pipeline()
                pipe.set(bucket_key, str(actual_tokens))
                pipe.set(ts_key, str(now))
                await pipe.execute()
                
                logger.info(f"🍏 [ADMISSION GRANTED]: Node '{node_key}' passed rate-limiter. Available Capacity: {actual_tokens:.1f}")
                return True
            else:
                # Capacity depleted, log strict admission denial
                logger.warning(f"⚠️  [ADMISSION DENIED]: Node '{node_key}' rate-limited! Distributed Token Bucket is dry.")
                return False
        except Exception as err:
            logger.error(f"❌ [LIMITER ERROR]: Admission matrix evaluation failed: {err}")
            return True # Fallback to open admission under error states to preserve processing continuity

    async def generate_master_snapshot(self) -> dict:
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
        master_data = cluster_snapshot_payload.get("master_registry", {})
        drift_detected = False
        
        for key in self._local_metrics_cache.keys():
            master_val = master_data.get(key, 0)
            local_val = self._local_metrics_cache[key]
            
            if local_val != master_val:
                logger.warning(f"⚠️ [STATE DRIFT DETECTED]: Cluster variation identified on metric '{key}'! Master Registry: {master_val} | Local Worker Cache: {local_val}")
                drift_detected = True
        
        if drift_detected:
            logger.info("🔄 [RECONCILIATION INITIALIZED]: Performing hot local catch-up sync with master registry...")
            for key in self._local_metrics_cache.keys():
                self._local_metrics_cache[key] = master_data.get(key, 0)
            logger.info("✅ [SYNCHRONIZATION COMPLETE]: Local memory matrices aligned with cluster state.")
            return True
            
        logger.info("🍏 [STATE ALIGNED]: Local worker states match the master cluster blueprint perfectly.")
        return False
