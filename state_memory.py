import asyncio
import json
import logging
import time
import uuid
from datetime import datetime

logger = logging.getLogger("StateMemory")

class AgentStateMemory:
    def __init__(self, redis_client=None):
        """Initializes Day 30 Split-State Cache Memory with Distributed Tracing capabilities."""
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

    async def generate_trace_context(self, parent_trace_id: str = None) -> dict:
        """
        Day 30 Core Feature: Distributed Context Propagator.
        Generates unique tracking headers to anchor distributed async transactions.
        """
        trace_id = parent_trace_id if parent_trace_id else str(uuid.uuid4())
        span_id = str(uuid.uuid4())[:8]
        return {
            "trace_id": trace_id,
            "span_id": span_id,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

    async def log_traced_event(self, trace_context: dict, node_key: str, status: str):
        """Prints a structured trace header logging entry to simulate unified telemetry maps."""
        t_id = trace_context.get("trace_id")
        s_id = trace_context.get("span_id")
        print(f"👁️  [TRACE MATRIX] TraceID: {t_id} | SpanID: {s_id} | Node: {node_key} -> Status: {status}")

    async def get_local_metric(self, metric_key: str) -> int:
        return self._local_metrics_cache.get(metric_key, 0)

    async def force_local_increment(self, metric_key: str, increment_value: int = 1):
        if metric_key in self._local_metrics_cache:
            self._local_metrics_cache[metric_key] += increment_value

    async def check_circuit_status(self, node_key: str, failure_threshold: int = 3, cooldown_sec: float = 3.0) -> str:
        try:
            state_key = f"afaos:circuit:state:{node_key}"
            pipe = self.client.pipeline()
            pipe.get(state_key)
            res = await pipe.execute()
            
            def decode_str(v, default):
                if v is None: return default
                return v.decode('utf-8') if isinstance(v, bytes) else str(v)
            return decode_str(res[0], "CLOSED")
        except Exception:
            return "CLOSED"

    async def report_node_execution_result(self, node_key: str, is_success: bool, failure_threshold: int = 3):
        try:
            state_key = f"afaos:circuit:state:{node_key}"
            if is_success:
                await self.client.set(state_key, "CLOSED")
            else:
                await self.client.set(state_key, "OPEN")
        except Exception:
            pass

    async def evaluate_admission_allowance(self, node_key: str, max_tokens: int = 5, refill_rate_per_sec: float = 2.0) -> bool:
        return True

    async def generate_master_snapshot(self) -> dict:
        return {"master_registry": self._local_metrics_cache}

    async def evaluate_and_reconcile_drift(self, cluster_snapshot_payload: dict) -> bool:
        return False
