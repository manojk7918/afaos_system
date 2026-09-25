import asyncio
import json
import redis.asyncio as aioredis

async def main():
    client = aioredis.from_url("redis://localhost:6379")
    payload = json.dumps({"event_type": "audit", "amount": 5000, "status": "pending"})
    await client.publish("afaos:events:financial", payload)
    print("JSON Event Published Successfully!")
    await client.aclose()

asyncio.run(main())
