import asyncio
import time
import logging
from redis.asyncio import Redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DistributedRateLimiter:
    def __init__(self, redis_url="redis://127.0.0.1:6379"):
        self.redis_url = redis_url
        self.redis = None

    async def initialize(self):
        """Establish connection pools to the memory cache layer."""
        self.redis = Redis.from_url(self.redis_url, decode_responses=True)
        logging.info("⚙️ Rate-Limiting Gatekeeper initialized.")

    async def is_allowed(self, client_id: str, max_requests: int = 5, window_seconds: int = 10) -> bool:
        """
        Validates if a specific transaction client can proceed using a sliding window.
        Returns True if allowed, False if throttled.
        """
        current_time = time.time()
        key = f"rate_limit:{client_id}"
        
        # Clear out requests that fell outside the old sliding time window
        clear_before = current_time - window_seconds
        
        try:
            async with self.redis.pipeline(transaction=True) as pipe:
                # Remove expired timestamps
                pipe.zremrangebyscore(key, 0, clear_before)
                # Count remaining active hits inside the window
                pipe.zcard(key)
                # Record current transaction timestamp
                pipe.zadd(key, {str(current_time): current_time})
                # Set a sliding expiration on the set itself to optimize memory
                pipe.expire(key, window_seconds + 5)
                
                # Execute pipeline atomically
                _, current_requests, _, _ = await pipe.execute()
                
            if current_requests > max_requests:
                logging.warning(f"🚨 [RATE LIMIT EXCEEDED]: Client '{client_id}' throttled! Current count: {current_requests}/{max_requests}")
                return False
                
            logging.info(f"✅ [REQUEST ALLOWED]: Client '{client_id}' passed gatekeeper check. Count: {current_requests}/{max_requests}")
            return True
            
        except Exception as e:
            logging.error(f"❌ Rate limiter fallback failure: {e}")
            return True # Fail-open in production to maintain service availability

    async def close(self):
        if self.redis:
            await self.redis.aclose()
            logging.info("🔒 Rate limiter connections dropped cleanly.")

async def main():
    limiter = DistributedRateLimiter()
    await limiter.initialize()
    
    # Simulate a rapid traffic burst from client 'orchestrator_node_1'
    client = "orchestrator_node_1"
    print(f"\n🚀 Launching rate-limiting burst simulation for: {client}...")
    
    try:
        for i in range(7):
            allowed = await limiter.is_allowed(client_id=client, max_requests=5, window_seconds=10)
            if not allowed:
                print(f"🛑 Drop packet {i+1}: Rate limit active.")
            await asyncio.sleep(0.2) # Fast burst hitting the gatekeeper
    finally:
        await limiter.close()

if __name__ == "__main__":
    asyncio.run(main())
