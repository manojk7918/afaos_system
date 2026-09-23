import asyncio
import logging
import sqlite3
import sys
from redis.asyncio import Redis
from circuit_breaker import DistributedCircuitBreaker
from rate_limiter import DistributedRateLimiter

# Configure structured logging for the final master deployment
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class SystemHealthMonitorDaemon:
    def __init__(self, redis_url="redis://127.0.0.1:6379", db_path="afaos_audit.db"):
        self.redis_url = redis_url
        self.db_path = db_path
        self.redis = None
        self.rate_limiter = None
        
    async def initialize(self):
        """Bind connection layers to verify system topography."""
        self.redis = Redis.from_url(self.redis_url, decode_responses=True)
        self.rate_limiter = DistributedRateLimiter(redis_url=self.redis_url)
        await self.rate_limiter.initialize()
        logging.info("🔮 AFAOS Cluster Master Health Monitor Daemon reporting for duty.")

    async def verify_infrastructure_health(self) -> bool:
        """
        Executes a 4-point cross-layer diagnostic sweep across volatile RAM and disk nodes.
        """
        system_ok = True
        print("\n" + "="*70)
        print(" 🔍 INITIALIZING CORE AFAOS COGNITIVE ENGINE STRUCTURAL SWEEP")
        print("="*70)

        # Diagnostic 1: Volatile Memory Layer Integrity (Redis)
        try:
            ping_response = await self.redis.ping()
            if ping_response:
                print(" ⚡ [MEMORY LAYER]: Connected to Redis Cluster Host -> STATUS: HEALTHY")
            else:
                print(" ⚡ [MEMORY LAYER]: Degraded Ping Response -> STATUS: UNSTABLE")
                system_ok = False
        except Exception as e:
            print(f" ⚡ [MEMORY LAYER]: Critical Socket Fault: {e} -> STATUS: CRITICAL")
            system_ok = False

        # Diagnostic 2: Permanent Ledger Storage Layer Integrity (SQLite Schema Validation)
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(execution_audit_logs);")
            columns = [col[1] for col in cursor.fetchall()]
            conn.close()

            # Confirm Day 34 alignment mapping columns exist
            required_cols = ["id", "workflow_id", "node_key", "assigned_agent", "completion_status", "instance_id"]
            if all(col in columns for col in required_cols):
                print(f" 🏛️  [LEDGER LAYER]: Relational DB Schema Verified Clean ({len(columns)} fields) -> STATUS: HEALTHY")
            else:
                print(" 🏛️  [LEDGER LAYER]: Discovered Schema Mismatch Drift -> STATUS: CORRUPTED")
                system_ok = False
        except Exception as e:
            print(f" 🏛️  [LEDGER LAYER]: SQLite Engine Unreadable: {e} -> STATUS: CRITICAL")
            system_ok = False

        # Diagnostic 3: Distributed Ingestion Gatekeeper (Rate Limiter)
        try:
            # Run an atomic, non-blocking check evaluation frame
            test_allowed = await self.rate_limiter.is_allowed("health_check_probe", max_requests=10, window_seconds=5)
            if test_allowed:
                print(" 🔒 [ADMISSION CONTROL]: Sliding-Window Rate Gatekeeper Operational -> STATUS: HEALTHY")
            else:
                print(" 🔒 [ADMISSION CONTROL]: Token/Sliding Window Locked Ingestion Stream -> STATUS: THROTTLED")
        except Exception as e:
            print(f" 🔒 [ADMISSION CONTROL]: Gatekeeper Pool Unreachable: {e} -> STATUS: DEGRADED")
            system_ok = False

        # Diagnostic 4: Out-of-Band Resilience Frameworks (Circuit Breaker Machine States)
        try:
            breaker = DistributedCircuitBreaker(service_name="cloud_dispatch_api", redis_url=self.redis_url)
            await breaker.initialize()
            state = await breaker.get_current_state()
            await breaker.close()
            
            if state == "CLOSED":
                print(f" ❇️  [RESILIENCE MATRIX]: External Dispatch Node State is '{state}' -> STATUS: HEALTHY")
            elif state == "HALF-OPEN":
                print(f" ❇️  [RESILIENCE MATRIX]: External Dispatch Node State is '{state}' -> STATUS: WARNING (CANARY ACTIVE)")
            else:
                print(f" ❇️  [RESILIENCE MATRIX]: External Dispatch Node State is '{state}' -> STATUS: TRIPPED (OPEN)")
        except Exception as e:
            print(f" ❇️  [RESILIENCE MATRIX]: Circuit Breaker Matrix Engine Fault: {e} -> STATUS: DEGRADED")
            system_ok = False

        print("="*70)
        return system_ok

    async def close(self):
        """Cleanly dump system allocations."""
        if self.redis:
            await self.redis.aclose()
        if self.rate_limiter:
            await self.rate_limiter.close()
        logging.info("🔒 System Health Monitor shut down cleanly.")

async def main():
    monitor = SystemHealthMonitorDaemon()
    await monitor.initialize()
    try:
        is_healthy = await monitor.verify_infrastructure_health()
        if is_healthy:
            print("\n 🎉 [SYSTEM DECLARATION]: ALL PIPELINES CLEAR! AFAOS SYSTEM IS 100% OPERATIONAL.")
            print(" Day 38 Core Build Cycle: COMPLETE.\n")
        else:
            print("\n ❌ [SYSTEM DECLARATION]: SYSTEM BOOT FAIL! Check degraded modules listed above.\n")
            sys.exit(1)
    finally:
        await monitor.close()

if __name__ == "__main__":
    asyncio.run(main())
