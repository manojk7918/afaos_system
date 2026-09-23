# projects/afaos_system/telemetry_dashboard.py
import json
import logging
import time
from typing import List
from state_memory import AgentStateMemory

# Configure clean logging for our dashboard engine
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AFAOS_Telemetry")

class TelemetryDashboard:
    """Streams and visualizes live multi-agent states directly inside the terminal window."""
    
    def __init__(self):
        # Bind directly to our existing Day 14 Redis memory engine
        self.memory = AgentStateMemory()

    def render_active_fleet_status(self, session_id: str) -> None:
        """Queries Redis infrastructure and prints a clean, structured status dashboard."""
        print("\n" + "="*60)
        print(f"📡 AFAOS LIVE AGENT TELEMETRY STREAM | SESSION: {session_id}")
        print("="*60)
        
        # 1. Fetch all unique agents registered to this workflow session
        active_agents: List[str] = self.memory.get_active_agents(session_id)
        
        if not active_agents:
            print("   [!] No active agent fleet components detected in telemetry store.")
            print("="*60 + "\n")
            return

        # 2. Iterate through each discovered agent and extract its real-time payload data
        for agent_role in active_agents:
            raw_state = self.memory.get_agent_state(session_id, agent_role)
            
            if raw_state and "payload_data" in raw_state:
                data = raw_state["payload_data"]
                # Extract structured values or fallback to default strings if not present
                status = data.get("status", "UNKNOWN")
                current_task = data.get("current_task", data.get("current_step", "N/A"))
                agent_type = data.get("agent_type", "Core_Component")
                
                # Assign simple clean visual icons for scannability based on status text
                icon = "🟢" if status in ["SUCCESS", "processing_complete", "monitoring"] else "⚡"
                
                print(f" {icon} ROLE: {agent_role:<25} | TYPE: {agent_type:<25}")
                print(f"    STATUS: {status:<23} | TASK: {current_task}")
                print("-" * 60)
                
        print(f"⏱️ Telemetry frame generated at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*60 + "\n")

# --- Interactive Test Loop ---
if __name__ == "__main__":
    print("\n--- Booting Day 16 Telemetry Streaming Monitor Engine ---")
    dashboard = TelemetryDashboard()
    
    # Target our standard verified system session UUID
    target_session = "fa15b023-5e8c-411a-bd63-902fd7b8e1a4"
    
    print(f"\n[Dashboard Active]  Monitoring target session cluster. Press Ctrl+C to exit.")
    try:
        # Run a continuous loop that refreshes the console visualization frame every 3 seconds
        while True:
            dashboard.render_active_fleet_status(target_session)
            time.sleep(3)
    except KeyboardInterrupt:
        print("\n🔌 Telemetry streaming monitor disconnected cleanly.")
