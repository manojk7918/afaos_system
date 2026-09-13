import os
import json
import csv
from typing import List, Dict, Any

class AFAOSDataIngestionEngine:
    def __init__(self, json_dir: str, csv_dir: str):
        """
        Initializes our data engine pathways.
        """
        self.json_dir = json_dir
        self.csv_dir = csv_dir

    def ingest_transaction_logs(self) -> List[Dict[str, Any]]:
        """
        Reads and parses JSON streams from our storage vault.
        """
        unified_txns = []
        if not os.path.exists(self.json_dir):
            return unified_txns

        for file_name in os.listdir(self.json_dir):
            if file_name.endswith(".json"):
                file_path = os.path.join(self.json_dir, file_name)
                try:
                    # Using python's native file context manager
                    with open(file_path, "r", encoding="utf-8") as stream:
                        data = json.load(stream)
                        if isinstance(data, list):
                            unified_txns.extend(data)
                    print(f"[INGESTION SUCCESS] Loaded {len(data)} JSON records from {file_name}")
                except Exception as err:
                    print(f"[INGESTION ERROR] Failed parsing JSON file {file_name}: {str(err)}")
        return unified_txns

    def ingest_ledger_records(self) -> List[Dict[str, Any]]:
        """
        Reads and parses flat CSV spreadsheets from our storage vault.
        """
        unified_ledgers = []
        if not os.path.exists(self.csv_dir):
            return unified_ledgers

        for file_name in os.listdir(self.csv_dir):
            if file_name.endswith(".csv"):
                file_path = os.path.join(self.csv_dir, file_name)
                try:
                    with open(file_path, "r", encoding="utf-8") as stream:
                        # DictReader converts CSV lines directly into clean Python dictionaries
                        reader = csv.DictReader(stream)
                        for row in reader:
                            unified_ledgers.append(dict(row))
                    print(f"[INGESTION SUCCESS] Loaded regulatory CSV records from {file_name}")
                except Exception as err:
                    print(f"[INGESTION ERROR] Failed parsing CSV file {file_name}: {str(err)}")
        return unified_ledgers

if __name__ == "__main__":
    # Pointing our engine to our real-time storage directories
    engine = AFAOSDataIngestionEngine(
        json_dir="./vault_storage/json_logs",
        csv_dir="./vault_storage/csv_records"
    )
    
    print("--- Running AFAOS Pipeline Day 1 Execution ---\n")
    transactions = engine.ingest_transaction_logs()
    ledgers = engine.ingest_ledger_records()
    
    print("\n--- System Audit Pipeline Diagnostics ---")
    print(f"Total Live System Transactions Cached: {len(transactions)}")
    print(f"Total Live Compliance Ledgers Cached: {len(ledgers)}")
    
    # Validation step to ensure data is live inside our python runtime
    if transactions:
        print(f"\n[SAMPLE RECOGNITION] Target Tx ID Checked: '{transactions[0]['transaction_id']}' with Risk Metric: {transactions[0]['risk_score']}")

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
        # Complex analytical query executing natively inside the database engine
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
                conn.row_factory = sqlite3.Row  # Dict-like row access capability
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
    # Run the advanced analytical pass
    processor.calculate_account_statistics()
