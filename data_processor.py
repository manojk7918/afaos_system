import sqlite3
from collections import deque
from typing import List, Dict, Any

class FinancialDataProcessor:
    def __init__(self, max_buffer_size: int = 5):
        self.processing_buffer = deque(maxlen=max_buffer_size)
        self.db_path = "afaos_cache.db"

    def calculate_account_statistics(self) -> List[Dict[str, Any]]:
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Aligned to your actual database schema: transaction_logs
            cursor.execute("SELECT * FROM transaction_logs;")
            records = cursor.fetchall()
            
            for rec in records:
                self.processing_buffer.append({"record_data": rec})
                
            conn.close()
            print(f"📊 [PROCESSOR SUCCESS]: Successfully synchronized memory buffer with {len(records)} records.")
            return list(self.processing_buffer)

        except sqlite3.OperationalError as e:
            print(f"\n⚠️ [PROCESSOR CRITICAL]: Schema mismatch or missing table.")
            print(f"🔬 [ENGINE DETAILS]: {str(e)}")
            return [{"status": "METRICS_UNAVAILABLE", "reason": "UNINITIALIZED_SCHEMA"}]

if __name__ == "__main__":
    processor = FinancialDataProcessor()
    processor.calculate_account_statistics()
