# projects/afaos_system/state_memory.py
import json
import logging
from typing import Dict, Any, Optional, List
import redis

# Configure clean logging for system tracking
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AFAOS_State_Memory")

class AgentStateMemory:
    """Manages transactional agent memory blocks inside the redis-stack infrastructure."""
    
    def __init__(self, host: str = "127.0.0.1", port: int = 6379, db: int = 0):
        # We connect directly via localhost because Docker forwards container port 6379 to your host machine
        self.client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self._verify_connection()

    def _verify_connection(self) -> None:
        """Pings the infrastructure node to ensure port connectivity."""
        try:
            if self.client.ping():
                logger.info("Successfully bound to redis-stack-server database node.")
        except redis.ConnectionError as e:
            logger.error(f"Infrastructure connection failed on port 6379. Details: {e}")
            raise

    def set_agent_state(self, session_id: str, agent_role: str, state_data: Dict[str, Any]) -> bool:
        """
        Saves structured state metadata linked to a specific session and role.
        Stores data as a serialized JSON string for atomic updates.
        """
        redis_key = f"afaos:state:{session_id}:{agent_role}"
        try:
            # Injecting base tracking properties
            payload = {
                "session_id": session_id,
                "agent_role": agent_role,
                "payload_data": state_data
            }
            # Commit atomic data packet with 24-hour retention safety layer (86400 seconds)
            self.client.set(redis_key, json.dumps(payload), ex=86400)
            logger.info(f"State memory successfully synchronized for key: {redis_key}")
            # Automatically link this agent to the global session index upon update
            self.register_active_agent(session_id, agent_role)
            return True
        except Exception as e:
            logger.error(f"Failed to commit state matrix to infrastructure: {e}")
            return False

    def get_agent_state(self, session_id: str, agent_role: str) -> Optional[Dict[str, Any]]:
        """Retrieves and unpacks the target agent state ledger."""
        redis_key = f"afaos:state:{session_id}:{agent_role}"
        try:
            raw_data = self.client.get(redis_key)
            if not raw_data:
                logger.warning(f"No active execution state matrix found for key: {redis_key}")
                return None
            return json.loads(raw_data)
        except Exception as e:
            logger.error(f"Error fetching execution matrix from infrastructure: {e}")
            return None

    # === STEP 2 EXTENSIONS: SESSION FLEET TRACKING ===

    def register_active_agent(self, session_id: str, agent_role: str) -> None:
        """Appends an active agent role to the session's universal index set."""
        session_set_key = f"afaos:session:{session_id}:active_agents"
        try:
            self.client.sadd(session_set_key, agent_role)
            # Set set-key expiration to match the state window (24 hours)
            self.client.expire(session_set_key, 86400)
        except Exception as e:
            logger.error(f"Failed to register agent role in global session index: {e}")

    def get_active_agents(self, session_id: str) -> List[str]:
        """Retrieves a flat list of all registered operational agents inside the session."""
        session_set_key = f"afaos:session:{session_id}:active_agents"
        try:
            agents = self.client.smembers(session_set_key)
            return list(agents) if agents else []
        except Exception as e:
            logger.error(f"Failed to fetch session index from infrastructure: {e}")
            return []

    # === STEP 3 EXTENSIONS: MANIPULATION (UPDATE & DELETE) ===

    def update_agent_status(self, session_id: str, agent_role: str, new_status: str) -> bool:
        """
        Explicitly updates just the status string within an existing agent's state payload.
        This pulls, updates, and re-saves the data packet.
        """
        # 1. Fetch current data matrix
        current_state = self.get_agent_state(session_id, agent_role)
        if current_state and "payload_data" in current_state:
            target_data = current_state["payload_data"]
        else:
            # If it doesn't exist, create a fresh payload structure
            target_data = {}
            
        # 2. Modify/Update the target field
        target_data["status"] = new_status
        
        # 3. Save it back to update the database
        logger.info(f"Updating agent status to: {new_status}")
        return self.set_agent_state(session_id, agent_role, target_data)

    def delete_agent_state(self, session_id: str, agent_role: str) -> bool:
        """Manually deletes a specific agent's state memory from the container."""
        redis_key = f"afaos:state:{session_id}:{agent_role}"
        try:
            # The .delete() command returns 1 if successfully deleted, 0 if key wasn't found
            result = self.client.delete(redis_key)
            if result:
                logger.info(f"Successfully executed delete operation on key: {redis_key}")
                return True
            logger.warning(f"Delete operation skipped. Key not found: {redis_key}")
            return False
        except Exception as e:
            logger.error(f"Failed to delete target key from infrastructure: {e}")
            return False

    def delete_entire_session(self, session_id: str) -> None:
        """Deletes the global active agent index tracking list for a session."""
        session_set_key = f"afaos:session:{session_id}:active_agents"
        try:
            self.client.delete(session_set_key)
            logger.info(f"Successfully deleted universal session registration index: {session_set_key}")
        except Exception as e:
            logger.error(f"Failed to delete session tracker index: {e}")


# --- Consolidated Day 14 Integration Sandbox Loop ---
if __name__ == "__main__":
    print("\n--- Running Consolidated Day 14 System Verification Loop ---")
    memory_manager = AgentStateMemory()
    
    test_session = "session_2026_09_22"
    test_role = "Financial_Compliance_Agent"
    
    # 1. Initial State Execution (Creation)
    print("\n[Step 1: Committing initial agent structure...]")
    mock_ledger_state = {
        "current_task": "validating_system_environment_variables",
        "loop_count": 1,
        "infrastructure_nodes_active": ["redis", "ledger", "vector_vault"],
        "status": "initializing"
    }
    memory_manager.set_agent_state(test_session, test_role, mock_ledger_state)
    
    # 2. Multi-Agent Index Registration Verification (Step 2 Check)
    print("\n[Step 2: Simulating background cluster fleet registration...]")
    memory_manager.set_agent_state(test_session, "Data_Ingestion_Agent", {"status": "processing"})
    memory_manager.set_agent_state(test_session, "Validation_Agent", {"status": "idle"})
    
    active_fleet = memory_manager.get_active_agents(test_session)
    print("Discovered Active Fleet Components:", active_fleet)
    
    # 3. Target State Data Modification (Step 3 UPDATE Check)
    print("\n[Step 3: Executing an explicit UPDATE operation...]")
    memory_manager.update_agent_status(test_session, test_role, "processing_complete")
    print("Verified Updated Payload:", memory_manager.get_agent_state(test_session, test_role))
    
    # 4. Target Data Clearance (Step 3 DELETE Check)
    print("\n[Step 4: Executing a manual DELETE operation...]")
    memory_manager.delete_agent_state(test_session, test_role)
    print("Verified Deleted Payload (Should show None):", memory_manager.get_agent_state(test_session, test_role))
    
    # Clean up secondary simulation data keys to maintain a clean database state
    memory_manager.delete_agent_state(test_session, "Data_Ingestion_Agent")
    memory_manager.delete_agent_state(test_session, "Validation_Agent")
    memory_manager.delete_entire_session(test_session)
    print("\n✅ End-to-End System Flow Complete.")
