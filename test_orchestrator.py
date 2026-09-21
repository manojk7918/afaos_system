import unittest
import sqlite3
import redis
import orchestrator
from orchestrator import run_persistent_orchestrator, initialize_system_infrastructure, SYSTEM_ROUTING_MATRIX, RELATIONAL_DB_PATH

class TestAFAOSOrchestrator(unittest.IsolatedAsyncioTestCase):
    
    async def asyncSetUp(self):
        """Set up and isolate infrastructure states cleanly before each test runs."""
        # 1. Force clear any lingering global connections from previous manual runs
        orchestrator.global_redis_pool = None
        
        # 2. Initialize fresh infrastructure bindings natively inside this loop
        await initialize_system_infrastructure()
        
        # 3. Use a clean sync client to flush old states out of Redis completely
        self.redis_sync = redis.Redis(host='localhost', port=6379, decode_responses=True)
        self.test_uuid = "test-uuid-9999-integration-spec"
        for key in SYSTEM_ROUTING_MATRIX.keys():
            try:
                self.redis_sync.delete(f"workflow:{self.test_uuid}:{key}")
            except Exception:
                pass

    async def test_end_to_end_orchestration_integrity(self):
        """Verify that the system processes correctly and commits logs safely."""
        # Execute the orchestrator framework inside the test container loop
        await run_persistent_orchestrator(self.test_uuid, SYSTEM_ROUTING_MATRIX)
        
        # 1. Validate Redis Caching Records
        ingest_state = self.redis_sync.get(f"workflow:{self.test_uuid}:Ingestion_Node")
        vector_state = self.redis_sync.get(f"workflow:{self.test_uuid}:Vector_Indexing_Node")
        analysis_state = self.redis_sync.get(f"workflow:{self.test_uuid}:Analysis_Agent_Node")
        dispatch_state = self.redis_sync.get(f"workflow:{self.test_uuid}:Cloud_Dispatch_Node")
        
        self.assertEqual(ingest_state, "SUCCESS", "Ingestion_Node state failed.")
        self.assertEqual(vector_state, "SUCCESS", "Vector_Indexing_Node state failed.")
        self.assertEqual(analysis_state, "SUCCESS", "Analysis_Agent_Node state failed.")
        self.assertEqual(dispatch_state, "SUCCESS", "Cloud_Dispatch_Node state failed.")

        # 2. Validate Relational Audit Logging Data
        conn = sqlite3.connect(RELATIONAL_DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT completion_status FROM execution_audit_logs WHERE workflow_id = ? AND node_key = ?",
            (self.test_uuid, "Analysis_Agent_Node")
        )
        row = cursor.fetchone()
        conn.close()
        
        self.assertIsNotNone(row, "Relational audit record missing from disk.")
        self.assertIn("SUCCESS", row[0], "SQL register failed to log active tracking state.")

if __name__ == "__main__":
    unittest.main()
