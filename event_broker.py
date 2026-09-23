import asyncio
import json
import logging
from typing import Callable, Awaitable, Dict, Any, Optional
import redis.asyncio as aioredis
import redis.exceptions as redis_exceptions

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("AFAOS_EventBroker")

REDIS_URL = "redis://localhost:6379"
STREAM_KEY = "afaos:stream:event_ledger"
DLQ_KEY = "afaos:queue:dead_letter"
BROADCAST_CHANNEL = "afaos:events:broadcast"

class EventBroker:
    def __init__(self, redis_client: Optional[Any] = None, redis_url: str = REDIS_URL):
        self.redis_url = redis_url
        self.redis = redis_client
        self._pubsub = None
        self._running = False

    async def connect(self):
        if not self.redis:
            self.redis = aioredis.from_url(self.redis_url, decode_responses=False)
            logger.info("Connected to Redis Event Broker.")

    async def initialize_consumer_group(self, stream_key: str = STREAM_KEY, group_name: str = "afaos:group:orchestrator_workers"):
        await self.connect()
        try:
            try:
                res = self.redis.xgroup_create(stream_key, group_name, id="0", mkstream=True)
                if asyncio.iscoroutine(res) or hasattr(res, "__await__"):
                    await res
                logger.info(f"👥 [CONSUMER GROUP INITIALIZED]: Created group '{group_name}' on stream '{stream_key}'.")
            except (redis_exceptions.ResponseError, Exception) as e:
                if "BUSYGROUP" in str(e):
                    logger.info(f"👥 [CONSUMER GROUP ACTIVE]: Group '{group_name}' already exists.")
                else:
                    raise e
        except Exception as e:
            logger.error(f"Failed to initialize consumer group: {e}")
            raise

    async def publish_event(self, topic: str, payload: Dict[str, Any]):
        await self.connect()
        try:
            raw_payload = json.dumps(payload).encode("utf-8")
            res = self.redis.xadd(STREAM_KEY, {"topic": topic.encode("utf-8"), "data": raw_payload})
            entry_id = await res if (asyncio.iscoroutine(res) or hasattr(res, "__await__")) else res
            entry_id_str = entry_id.decode("utf-8") if isinstance(entry_id, bytes) else str(entry_id)
            
            broadcast_msg = json.dumps({"topic": topic, "stream_id": entry_id_str, "payload": payload})
            pub_res = self.redis.publish(BROADCAST_CHANNEL, broadcast_msg.encode("utf-8"))
            if asyncio.iscoroutine(pub_res) or hasattr(pub_res, "__await__"):
                await pub_res
            
            logger.info(f"Published event [{topic}] with ID {entry_id_str}")
            return entry_id_str
        except Exception as e:
            logger.error(f"Failed to publish event [{topic}]: {e}")
            raise

    async def quarantine_poison_payload(self, raw_data: bytes, error_msg: str):
        await self.connect()
        dlq_entry = {
            "error": error_msg,
            "raw_payload": raw_data.decode("utf-8", errors="replace")
        }
        res = self.redis.lpush(DLQ_KEY, json.dumps(dlq_entry).encode("utf-8"))
        if asyncio.iscoroutine(res) or hasattr(res, "__await__"):
            await res
        logger.warning(f"Quarantined payload into DLQ ({DLQ_KEY}): {error_msg}")

    async def subscribe_wildcard(self, pattern: str, callback: Callable[[str, Dict[str, Any]], Awaitable[None]]):
        await self.connect()
        if not self._pubsub:
            self._pubsub = self.redis.pubsub()
        
        res = self._pubsub.psubscribe(pattern)
        if asyncio.iscoroutine(res) or hasattr(res, "__await__"):
            await res

        self._running = True
        logger.info(f"Subscribed to wildcard pattern: {pattern}")

        while self._running:
            message = None
            try:
                # get_message is synchronous in redis-py asyncio pubsub
                raw_msg = self._pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                message = await raw_msg if (asyncio.iscoroutine(raw_msg) or hasattr(raw_msg, "__await__")) else raw_msg
                
                if message and message.get("type") in ("pmessage", "message"):
                    data = message["data"]
                    if isinstance(data, bytes):
                        data = data.decode("utf-8")
                    parsed = json.loads(data)
                    channel_name = message["channel"].decode("utf-8") if isinstance(message["channel"], bytes) else message["channel"]
                    await callback(channel_name, parsed)
            except Exception as err:
                if message and "data" in message:
                    await self.quarantine_poison_payload(
                        message["data"] if isinstance(message["data"], bytes) else str(message["data"]).encode("utf-8"),
                        str(err)
                    )
            await asyncio.sleep(0.01)

    start_pattern_listener = subscribe_wildcard

    async def close(self):
        self._running = False
        if self._pubsub:
            res = self._pubsub.unsubscribe()
            if asyncio.iscoroutine(res) or hasattr(res, "__await__"):
                await res
            res_close = self._pubsub.close()
            if asyncio.iscoroutine(res_close) or hasattr(res_close, "__await__"):
                await res_close
        if self.redis and hasattr(self.redis, "close"):
            res = self.redis.close()
            if asyncio.iscoroutine(res) or hasattr(res, "__await__"):
                await res
            logger.info("Event Broker disconnected gracefully.")

RedisEventBroker = EventBroker

if __name__ == "__main__":
    async def main():
        broker = EventBroker()
        await broker.connect()
        await broker.initialize_consumer_group()
        test_id = await broker.publish_event("afaos:events:sys.test", {"status": "HEALTHY", "workflow_id": "wf_init_001"})
        print(f"Test Event Stream Entry ID: {test_id}")
        await broker.close()

    asyncio.run(main())