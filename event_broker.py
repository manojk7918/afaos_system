import asyncio
import json
import logging
import inspect
from datetime import datetime

logger = logging.getLogger(__name__)

class RedisEventBroker:
    def __init__(self, redis_client):
        """Initializes the Event-Driven Broker with Day 23 Pattern-Matching Route Filters."""
        self.redis_client = redis_client
        self.broadcast_channel = "afaos:events:broadcast"
        self.dlq_key = "afaos:queue:dead_letter"
        self._listener_task = None
        self._is_listening = False

    async def _safe_execute(self, target_callable, *args, **kwargs):
        """Helper to invoke a method and safely await it only if it returns a coroutine."""
        result = target_callable(*args, **kwargs)
        if asyncio.iscoroutine(result) or inspect.isawaitable(result):
            return await result
        return result

    async def publish_event(self, topic: str, event_type: str, source_node: str, payload: dict) -> bool:
        """Constructs a structured event payload and streams it to channel slots."""
        event_envelope = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "source_node": source_node,
            "event_type": event_type,
            "payload": payload
        }
        
        try:
            serialized_data = json.dumps(event_envelope)
            specific_channel = f"afaos:events:{topic}"
            
            await self._safe_execute(self.redis_client.publish, specific_channel, serialized_data)
            await self._safe_execute(self.redis_client.publish, self.broadcast_channel, serialized_data)
            
            logger.info(f"📣 [PUB SUCCESS]: Event '{event_type}' streamed to channel rooms.")
            return True
        except Exception as err:
            logger.error(f"❌ [PUB ERROR]: Failed to serialize or stream event metadata packet: {err}")
            return False

    async def handle_dead_letter(self, failed_node: str, error_message: str, original_payload: dict):
        """Enqueues toxic payloads into the persistent Redis Dead-Letter Queue."""
        dlq_envelope = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "failed_node": failed_node,
            "error_reason": error_message,
            "poison_data": original_payload
        }
        try:
            serialized_dlq = json.dumps(dlq_envelope)
            await self._safe_execute(self.redis_client.lpush, self.dlq_key, serialized_dlq)
            logger.warning(f"🚨 [DLQ ENQUEUED]: Toxic execution packet isolated in dead-letter storage.")
            await self.publish_event(
                topic="Recovery_Engine",
                event_type="NODE_CRASH_RECOVERY_TRIGGERED",
                source_node="DLQ_Safety_Guard",
                payload={"targeted_fallback_node": failed_node, "action": "state_rollback_initiated"}
            )
        except Exception as dlq_err:
            logger.error(f"❌ [DLQ CRITICAL ERROR]: Failed to quarantine poison layout metrics: {dlq_err}")

    async def _listen_loop(self, pubsub_instance, callback_func):
        """Internal background consumer extraction loop parsing incoming data packets."""
        try:
            while self._is_listening:
                message = await self._safe_execute(pubsub_instance.get_message, ignore_subscribe_messages=True, timeout=1.0)
                if message and message.get("type") in ["message", "pmessage"]:
                    try:
                        raw_data = message["data"]
                        if isinstance(raw_data, bytes):
                            raw_data = raw_data.decode('utf-8')
                        
                        parsed_envelope = json.loads(raw_data)
                        if asyncio.iscoroutinefunction(callback_func):
                            await callback_func(parsed_envelope)
                        else:
                            callback_func(parsed_envelope)
                    except Exception as json_err:
                        logger.error(f"⚠️ [SUB PARSE ERROR]: Extraction failure on message data packet stream: {json_err}")
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            pass
        except Exception as loop_err:
            logger.error(f"❌ [SUB LOOP EXCEPTION]: Critical break in background event stream listener: {loop_err}")

    async def start_pattern_listener(self, pattern_target: str, callback_func):
        """
        Spins up a non-blocking persistent wildcard subscriber context 
        (psubscribe) to intercept events matching specific routing topologies.
        """
        if self._is_listening:
            return

        try:
            pubsub = self.redis_client.pubsub()
            await self._safe_execute(pubsub.psubscribe, pattern_target)
            
            self._is_listening = True
            self._listener_task = asyncio.create_task(self._listen_loop(pubsub, callback_func))
            logger.info(f"🛰️  [PATTERN SUB ACTIVE]: Background wildcard consumer bound cleanly to '{pattern_target}'.")
        except Exception as sub_err:
            logger.error(f"❌ [SUB INIT ERROR]: Failed to bind subscriber context pattern filters: {sub_err}")

    async def stop_pattern_listener(self):
        """Gracefully tears down and cancels background pattern subscriber tracking loops."""
        if not self._is_listening:
            return

        self._is_listening = False
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        logger.info("🔌 [SUB SHUTDOWN]: Pattern subscription listener context cleanly detached.")
