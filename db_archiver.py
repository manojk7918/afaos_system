# projects/afaos_system/db_archiver.py
import os
import shutil
import logging
from datetime import datetime
from typing import List

# Configure clean logging for tracking system operations
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AFAOS_DB_Archiver")

class DatabaseArchiver:
    """Manages hot-snapshot point-in-time recovery backups for the AFAOS relational audit database."""
    
    def __init__(self, db_path: str = "afaos_audit.db", backup_dir: str = "vault_storage/backups", max_backups: int = 5):
        self.db_path = db_path
        self.backup_dir = backup_dir
        self.max_backups = max_backups
        
        # Ensure backup vault directory structure exists on the disk
        os.makedirs(self.backup_dir, exist_ok=True)

    def execute_snapshot(self) -> str:
        """Creates an atomic timestamped backup copy of the core audit database."""
        if not os.path.exists(self.db_path):
            logger.error(f"Snapshot execution failed. Source database target not found: {self.db_path}")
            return ""

        try:
            # Generate a precise timestamp string for point-in-time tracking
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_filename = f"afaos_audit_snapshot_{timestamp}.db"
            destination_path = os.path.join(self.backup_dir, backup_filename)
            
            # Execute a clean, binary-safe file copy operation
            shutil.copy2(self.db_path, destination_path)
            logger.info(f"💾 Relational snapshot successfully archived at: {destination_path}")
            
            # Trigger the automatic cleanup routine to remove oldest backups
            self._rotate_backups()
            return destination_path
            
        except Exception as e:
            logger.error(f"Critical error during snapshot execution cycle: {e}")
            return ""

    def _rotate_backups(self) -> None:
        """Automated retention policy: Deletes the oldest backups if count exceeds max_backups limit."""
        try:
            # Read all snapshot files currently inside the backup vault directory
            all_files = [os.path.join(self.backup_dir, f) for f in os.listdir(self.backup_dir) if f.startswith("afaos_audit_snapshot_")]
            
            # Sort files by creation time (oldest first)
            all_files.sort(key=os.path.getctime)
            
            # If the current count is larger than our safety limit, delete the oldest entries
            while len(all_files) > self.max_backups:
                oldest_backup = all_files.pop(0)
                os.remove(oldest_backup)
                logger.warning(f"♻️ Retention policy rotation active: Deleted stale backup entry: {oldest_backup}")
                
        except Exception as e:
            logger.error(f"Failed to execute automated backup retention rotation: {e}")

# --- Verification Sandbox Loop ---
if __name__ == "__main__":
    print("\n--- Initializing Day 15 Database Archiver System Test ---")
    
    # Simulating the existence of a database file if it's missing in sandbox mode
    if not os.path.exists("afaos_audit.db"):
        with open("afaos_audit.db", "w") as f:
            f.write("MOCK_RELATIONAL_DATA_STREAM")
            
    # Initialize the archiver with a strict max retention limit of 3 for testing rotation quickly
    archiver = DatabaseArchiver(max_backups=3)
    
    import time
    print("\n[Step 1: Running multiple sequential snapshot iterations...]")
    archiver.execute_snapshot()
    time.sleep(1.1)
    archiver.execute_snapshot()
    time.sleep(1.1)
    archiver.execute_snapshot()
    
    time.sleep(1.1)
    print("\n[Step 2: Triggering 4th backup to test automated rotation mechanism...]")
    # This 4th snapshot will now trigger our retention rotation, deleting the 1st one automatically
    archiver.execute_snapshot()

    
    print("\n✅ Verification complete. Check your 'vault_storage/backups' folder to see the snapshots!")
