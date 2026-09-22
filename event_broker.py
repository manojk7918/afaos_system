import asyncio
import json
import logging
import inspect
from datetime import datetime

logger = logging.getLogger(__name__)

class RedisEventBroker:
    def __init__(self, redis_client):
        """Initializes the Stateful Event Broker with Day 25 Consumer Group capabilities."""
        self.redis_client = redis_client
        self.broadcast_channel = "afaos:events:broadcast"
        self.stream_ledger_key = "afaos:stream:event_ledger"
        self.dlq_key = "afaos:queue:dead_letter"
        self.consumer_group_name = "afaos:group:orchestrator_workers"
        self._listener_task = None
        self._is_listening = False

    async def _safe_execute(self, target_callable, *args, **kwargs):
        """Helper to invoke a method and safely await it only if it returns a coroutine."""
        result = target_callable(*args, **kwargs)
        if asyncio.iscoroutine(result) or inspect.isawaitable(result):
            return await result
        return result

    async def publish_event(self, topic: str, event_type: str, source_node: str, payload: dict) -> bool:
        """Constructs a structured event payload, streams it to Pub/Sub, and appends to Redis Stream."""
        event_envelope = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "source_node": source_node,
            "event_type": event_type,
            "payload": json.dumps(payload)
        }
        try:
            serialized_data = json.dumps(event_envelope)
            specific_channel = f"afaos:events:{topic}"
            
            await self._safe_execute(self.redis_client.publish, specific_channel, serialized_data)
            await self._safe_execute(self.redis_client.publish, self.broadcast_channel, serialized_data)
            
            await self._safe_execute(self.redis_client.xadd, self.stream_ledger_key, event_envelope, id="*")
            logger.info(f"📣 [PUB & STREAM SUCCESS]: Event '{event_type}' archived into persistent stream ledger.")
            return True
        except Exception as err:
            logger.error(f"❌ [PUB ERROR]: Failed to stream event metadata packet: {err}")
            return False

    async def initialize_consumer_group(self):
        """
        Day 25 Feature: Idempotently creates a Redis Consumer Group bound to the stream ledger.
        Uses '0' to read the stream from the very beginning.
        """
        try:
            # XGROUP CREATE <stream> <group> <id> MKSTREAM
            await self._safe_execute(
                self.redis_client.xgroup_create,
                self.stream_ledger_key,
                self.consumer_group_name,
                id="0",
                mkstream=True
            )
            logger.info(f"👥 [CONSUMER GROUP READY]: Group '{self.consumer_group_name}' initialized successfully.")
        except Exception as err:
            # Check if group already exists error (BUSYGROUP), which is expected on subsequent runs
            err_str = str(err)
            if "BUSYGROUP" in err_str or "already exists" in err_str:
                logger.info(f"ℹ️  [CONSUMER GROUP EXISTS]: Group '{self.consumer_group_name}' already active.")
            else:
                logger.error(f"❌ [GROUP INIT ERROR]: Failed to create consumer group allocation matrix: {err}")

    async def read_from_consumer_group(self, consumer_name: str, count: int = 1) -> list:
        """
        Day 25 Feature: Reads load-balanced messages allocated explicitly to this consumer 
        instance using XREADGROUP. Uses '>' to fetch new, unconsumed messages.
        """
        try:
            # XREADGROUP GROUP <group> <consumer> COUNT <count> STREAMS <stream> >
            raw_streams = await self._safe_execute(
                self.redis_client.xreadgroup,
                self.consumer_group_name,
                consumer_name,
                {self.stream_ledger_key: ">"},
                count=count,
                block=100  # Block for 100ms if no new events are present
            )
            
            parsed_entries = []
            if raw_streams:
                for stream_key, entries in raw_streams:
                    for entry_id, fields in entries:
                        id_str = entry_id.decode('utf-8') if isinstance(entry_id, bytes) else entry_id
                        
                        normalized_fields = {}
                        for k, v in fields.items():
                            key_str = k.decode('utf-8') if isinstance(k, bytes) else k
                            val_str = v.decode('utf-8') if isinstance(v, bytes) else v
                            
                            if key_str == "payload":
                                try:
                                    normalized_fields[key_str] = json.loads(val_str)
                                except:
                                    normalized_fields[key_str] = val_str
                            else:
                                normalized_fields[key_str] = val_str
                        
                        parsed_entries.append({"message_id": id_str, "data": normalized_fields})
                        
                        # Day 25 Acknowledgment (XACK): Inform Redis the message was handled safely
                        await self._safe_execute(
                            self.redis_client.xack,
                            self.stream_ledger_key,
                            self.consumer_group_name,
                            id_str
                        )
            return parsed_entries
        except Exception as err:
            logger.error(f"❌ [XREADGROUP ERROR]: Failed to pull metrics from consumer group matrix: {err}")
            return []

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
        """Internal background consumer loop parsing reactive Pub/Sub metrics messages."""
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
                        logger.error(f"⚠️ [SUB PARSE ERROR]: Extraction failure: {json_err}")
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            pass
        except Exception as loop_err:
            logger.error(f"❌ [SUB LOOP EXCEPTION]: {loop_err}")

    async def start_pattern_listener(self, pattern_target: str, callback_func):
        """Spins up a non-blocking persistent wildcard subscriber context."""
        if self._is_listening:
            return
        try:
            pubsub = self.redis_client.pubsub()
            await self._safe_execute(pubsub.psubscribe, pattern_target)
            self._is_listening = True
            self._listener_task = asyncio.create_task(self._listen_loop(pubsub, callback_func))
            logger.info(f"🛰️  [PATTERN SUB ACTIVE]: Background wildcard consumer bound cleanly to '{pattern_target}'.")
        except Exception as sub_err:
            logger.error(f"❌ [SUB INIT ERROR]: Failed to bind subscriber pattern: {sub_err}")

    async def stop_pattern_listener(self):
        """Gracefully tears down and cancels background subscriber tracking loops."""
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
