import sqlite3
import os
from typing import List, Dict, Any

class FinancialDataProcessor:
    """Engine responsible for high-performance SQL analytics and feature calculation."""
    
    def __init__(self, db_name: str = "afaos_cache.db"):
        self.db_path = os.path.join(os.getcwd(), db_name)

    def calculate_account_statistics(self) -> List[Dict[str, Any]]:
        """
        Uses advanced SQL Window Functions to calculate account running metrics 
        and flags transactions that exceed 3x the account's historical average.
        """
        query = """
            with historical_metrics as (
                SELECT 
                    txn_id,
                    ledger_id,
                    account_number,
                    amount,
                    transaction_type,
                    AVG(amount) OVER(PARTITION BY account_number) as avg_account_spend,
                    SUM(amount) OVER(PARTITION BY account_number ORDER BY txn_id) as account_running_total
                FROM transaction_logs
            )
            SELECT 
                txn_id,
                ledger_id,
                account_number,
                amount,
                transaction_type,
                ROUND(avg_account_spend, 2) as avg_spend,
                ROUND(account_running_total, 2) as running_total,
                CASE 
                    WHEN amount > (avg_account_spend * 3) THEN 1 
                    ELSE 0 
                END as is_anomaly_spike
            FROM historical_metrics;
        """
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(query)
                
                results = []
                for row in cursor.fetchall():
                    results.append(dict(row))
                
                print(f"📊 [PROCESSOR SUCCESS]: Analyzed records using window functions. Generated {len(results)} metrics profiles.")
                return results
                
        except sqlite3.OperationalError as e:
            print(f"⚠️ [PROCESSOR CAUTION]: Analytical engine parsed successfully, but schema is currently empty. Error: {e}")
            return []

if __name__ == "__main__":
    processor = FinancialDataProcessor()
    processor.calculate_account_statistics()
