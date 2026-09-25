import time
import sqlite3
import redis
from fastapi import FastAPI

app = FastAPI(title="AFAOS System Observability API")

START_TIME = time.time()
REDIS_CLIENT = redis.Redis(host="redis", port=6379, db=0, decode_responses=True)
DB_PATH = "/app/data/afaos_audit.db"

@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "uptime_seconds": round(time.time() - START_TIME, 2),
        "version": "1.0.0"
    }

@app.get("/metrics")
def get_metrics():
    # Read total events from SQLite
    total_events = 0
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM execution_audit_logs;")
        total_events = cursor.fetchone()[0]
        conn.close()
    except Exception:
        total_events = -1

    # Read stream queue length from Redis
    try:
        stream_length = REDIS_CLIENT.xlen("afaos:stream:event_ledger")
    except Exception:
        stream_length = -1

    return {
        "system_status": "ONLINE",
        "total_events_committed": total_events,
        "active_stream_queue_depth": stream_length,
        "system_uptime": f"{round(time.time() - START_TIME, 2)}s"
    }