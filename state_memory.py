import asyncio
import json
import logging
import time
from datetime import datetime

logger = logging.getLogger("StateMemory")

class AgentStateMemory:
    def __init__(self, redis_client=None):
        """Initializes Day 29 Split-State Cache Memory with Distributed Circuit Breaker capabilities."""
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

    async def check_circuit_status(self, node_key: str, failure_threshold: int = 3, cooldown_sec: float = 3.0) -> str:
        """
        Day 29 Core Feature: Distributed Circuit Breaker Matrix.
        Evaluates cluster infrastructure health state tokens before dropping socket hooks.
        Returns: "CLOSED", "OPEN", or "HALF_OPEN"
        """
        try:
            state_key = f"afaos:circuit:state:{node_key}"
            fail_cnt_key = f"afaos:circuit:failures:{node_key}"
            ts_key = f"afaos:circuit:last_trip:{node_key}"
            
            # Fetch breaker metrics atomically from the connection pool
            pipe = self.client.pipeline()
            pipe.get(state_key)
            pipe.get(fail_cnt_key)
            pipe.get(ts_key)
            res = await pipe.execute()
            
            def decode_str(v, default):
                if v is None: return default
                return v.decode('utf-8') if isinstance(v, bytes) else str(v)
                
            current_state = decode_str(res[0], "CLOSED")
            fail_count = int(decode_str(res[1], "0"))
            last_trip = float(decode_str(res[2], "0.0"))
            
            now = time.time()
            
            if current_state == "OPEN":
                # Check if the configured cooldown window has passed to attempt a half-open probe
                if now - last_trip >= cooldown_sec:
                    await self.client.set(state_key, "HALF_OPEN")
                    logger.warning(f"🟡 [CIRCUIT HALF-OPEN]: Cooldown window expired for '{node_key}'. Injecting canary test request...")
                    return "HALF_OPEN"
                return "OPEN"
                
            return current_state
        except Exception as err:
            logger.error(f"❌ [BREAKER ERROR]: Circuit check sequence faulted: {err}")
            return "CLOSED"

    async def report_node_execution_result(self, node_key: str, is_success: bool, failure_threshold: int = 3):
        """Updates the distributed state machine metric trackers based on pipeline transaction feedback."""
        try:
            state_key = f"afaos:circuit:state:{node_key}"
            fail_cnt_key = f"afaos:circuit:failures:{node_key}"
            ts_key = f"afaos:circuit:last_trip:{node_key}"
            
            if is_success:
                # Reset tracking metrics upon successful canary or stable runs
                pipe = self.client.pipeline()
                pipe.set(state_key, "CLOSED")
                pipe.set(fail_cnt_key, "0")
                await pipe.execute()
            else:
                # Increment failure counts atomically
                new_fails = await self.client.incr(fail_cnt_key)
                current_state = await self.client.get(state_key)
                current_state = current_state.decode('utf-8') if isinstance(current_state, bytes) else str(current_state or "CLOSED")
                
                if new_fails >= failure_threshold or current_state == "HALF_OPEN":
                    pipe = self.client.pipeline()
                    pipe.set(state_key, "OPEN")
                    pipe.set(ts_key, str(time.time()))
                    await pipe.execute()
                    logger.critical(f"💥 [CIRCUIT TRIPPED - OPEN]: Node '{node_key}' has crossed critical failure threshold ({new_fails})! Isolating cluster connections.")
        except Exception as err:
            logger.error(f"❌ [BREAKER RECORD ERROR]: Failed to log node result to state machine: {err}")

    async def evaluate_admission_allowance(self, node_key: str, max_tokens: int = 5, refill_rate_per_sec: float = 2.0) -> bool:
        try:
            bucket_key = f"afaos:limiter:tokens:{node_key}"
            ts_key = f"afaos:limiter:last_refill:{node_key}"
            now = time.time()
            
            pipe = self.client.pipeline()
            pipe.get(bucket_key)
            pipe.get(ts_key)
            res = await pipe.execute()
            
            def to_float(v, default):
                if v is None: return default
                return float(v.decode('utf-8') if isinstance(v, bytes) else v)
                
            current_tokens = to_float(res[0], float(max_tokens))
            last_refill = to_float(res[1], now)
            
            time_passed = now - last_refill
            actual_tokens = min(float(max_tokens), current_tokens + (time_passed * refill_rate_per_sec))
            
            if actual_tokens >= 1.0:
                actual_tokens -= 1.0
                pipe = self.client.pipeline()
                pipe.set(bucket_key, str(actual_tokens))
                pipe.set(ts_key, str(now))
                await pipe.execute()
                return True
            return False
        except Exception:
            return True

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
            if self._local_metrics_cache[key] != master_data.get(key, 0):
                drift_detected = True
        if drift_detected:
            for key in self._local_metrics_cache.keys():
                self._local_metrics_cache[key] = master_data.get(key, 0)
            return True
        return False
