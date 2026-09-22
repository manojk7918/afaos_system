import asyncio
import inspect
import logging
import uuid

logger = logging.getLogger(__name__)

class RedisDistributedLock:
    def __init__(self, redis_client, relational_workflow_uuid, lease_time_sec=60):
        """Initializes the distributed lock with an adaptive client validator."""
        self.redis_client = redis_client
        self.lock_key = f"lock:workflow:{relational_workflow_uuid}"
        self.lease_time_sec = lease_time_sec
        self.owner_id = str(uuid.uuid4())
        self.acquired = False

    async def acquire(self) -> bool:
        """Attempts to acquire the lock while safely handling both async and sync clients."""
        try:
            # NX: Only set key if it doesn't exist | PX: Set expiry time in milliseconds
            result = self.redis_client.set(
                self.lock_key, 
                self.owner_id, 
                nx=True, 
                px=int(self.lease_time_sec * 1000)
            )
            
            # Day 18 Fix: Check if the result is an awaitable object or a raw boolean
            if asyncio.iscoroutine(result) or inspect.isawaitable(result):
                success = await result
            else:
                success = result
                
            if success:
                self.acquired = True
                logger.info(f"🔒 [LOCK ACQUIRED]: Successfully locked key '{self.lock_key}' for instance {self.owner_id}")
                return True
            return False
        except Exception as err:
            logger.error(f"❌ [LOCK ERROR]: Failed during acquire execution phase: {err}")
            return False

    async def release(self):
        """Safely releases the lock only if this specific instance is the current lock owner."""
        if not self.acquired:
            return
        
        try:
            # Verify ownership safely across async/sync interface boundaries
            get_result = self.redis_client.get(self.lock_key)
            if asyncio.iscoroutine(get_result) or inspect.isawaitable(get_result):
                current_token = await get_result
            else:
                current_token = get_result
            
            if isinstance(current_token, bytes):
                current_token = current_token.decode('utf-8')

            if current_token == self.owner_id:
                delete_result = self.redis_client.delete(self.lock_key)
                if asyncio.iscoroutine(delete_result) or inspect.isawaitable(delete_result):
                    await delete_result
                logger.info(f"🔓 [LOCK RELEASED]: Cleanly freed resource index '{self.lock_key}'")
            
            self.acquired = False
        except Exception as err:
            logger.error(f"❌ [LOCK ERROR]: Failed during release execution phase: {err}")

    async def __aenter__(self):
        if not await self.acquire():
            raise RuntimeError("Could not acquire distributed concurrency lock. Session execution aborted.")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release()
