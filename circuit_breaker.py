import asyncio
import logging
import time
from redis.asyncio import Redis

# Configure structured logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DistributedCircuitBreaker:
    def __init__(self, service_name: str, redis_url="redis://127.0.0.1:6379", failure_threshold=3, recovery_timeout=5):
        self.redis_url = redis_url
        self.service_name = service_name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.redis = None
        
        # State tracking Redis keys
        self.state_key = f"afaos:circuit:state:{service_name}"
        self.fail_count_key = f"afaos:circuit:failures:{service_name}"
        self.last_state_change_key = f"afaos:circuit:last_change:{service_name}"

    async def initialize(self):
        """Establish asynchronous connection layers and establish the base state."""
        self.redis = Redis.from_url(self.redis_url, decode_responses=True)
        current_state = await self.redis.get(self.state_key)
        if not current_state:
            await self.redis.set(self.state_key, "CLOSED")
        logging.info(f"⚙️ Distributed Circuit Breaker Engine initialized for service: '{self.service_name}'.")

    async def get_current_state(self) -> str:
        """Fetch active machine configuration directly from Redis cache memory."""
        return await self.redis.get(self.state_key) or "CLOSED"

    async def can_execute(self) -> bool:
        """
        Validates whether a request pipeline frame should execute or short-circuit.
        Automatically switches states from OPEN to HALF-OPEN if the cooldown expires.
        """
        state = await self.get_current_state()
        
        if state == "CLOSED":
            return True
            
        if state == "OPEN":
            last_change = float(await self.redis.get(self.last_state_change_key) or 0)
            if time.time() - last_change > self.recovery_timeout:
                logging.warning(f"⏰ [COOLDOWN EXPIRED]: Circuit moving to HALF-OPEN for '{self.service_name}'. Sending canary probe.")
                await self.redis.set(self.state_key, "HALF-OPEN")
                await self.redis.set(self.last_state_change_key, str(time.time()))
                return True
            return False
            
        if state == "HALF-OPEN":
            return True
            
        return False

    async def record_success(self):
        """Resets structural error thresholds when dependencies respond safely."""
        state = await self.get_current_state()
        if state != "CLOSED":
            logging.info(f"❇️ [CIRCUIT RECOVERED]: Service '{self.service_name}' passed canary checks. Resetting to CLOSED.")
            await self.redis.set(self.state_key, "CLOSED")
        await self.redis.delete(self.fail_count_key)

    async def record_failure(self):
        """Increments error logs. Trips the circuit completely if boundaries break."""
        failures = await self.redis.incr(self.fail_count_key)
        state = await self.get_current_state()
        
        logging.warning(f"⚠️ [FAILURE DETECTED]: Service '{self.service_name}' error count: {failures}/{self.failure_threshold}")
        
        if failures >= self.failure_threshold or state == "HALF-OPEN":
            logging.error(f"🚨 [CIRCUIT TRIPPED]: Threshold breached! Tripping circuit to OPEN for '{self.service_name}'. Short-circuiting traffic.")
            await self.redis.set(self.state_key, "OPEN")
            await self.redis.set(self.last_state_change_key, str(time.time()))

    async def close(self):
        """Cleanly close connection pools."""
        if self.redis:
            await self.redis.aclose()
            logging.info("🔒 Circuit breaker cache connection dropped cleanly.")

async def main():
    # Target dummy service 'cloud_dispatch_ledger_api'
    breaker = DistributedCircuitBreaker(service_name="cloud_dispatch_api", failure_threshold=3, recovery_timeout=3)
    await breaker.initialize()
    
    print("\n🚀 Starting State-Machine Circuit Simulation...")
    try:
        # 1. Trigger consecutive failures to force an OPEN state trip
        for i in range(4):
            allowed = await breaker.can_execute()
            print(f"Call {i+1}: Allowed to execute? -> {allowed}")
            if allowed:
                # Simulate error happening downstream
                await breaker.record_failure()
            await asyncio.sleep(0.1)

        # 2. Assert short-circuit execution drops call immediately
        allowed_while_open = await breaker.can_execute()
        print(f"\nImmediate validation call during trip window: Allowed? -> {allowed_while_open} (Should be False)")

        # 3. Wait out the cooldown time step to verify canary recovery transitions
        print("\nSleeping 4 seconds to let the cooldown timeout lapse...")
        await asyncio.sleep(4)
        
        allowed_canary = await breaker.can_execute()
        print(f"Canary probe check: Allowed? -> {allowed_canary} (Should move to HALF-OPEN -> True)")
        if allowed_canary:
            # Simulate a successful execution confirming recovery
            await breaker.record_success()

    finally:
        await breaker.close()

if __name__ == "__main__":
    asyncio.run(main())
