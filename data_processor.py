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
