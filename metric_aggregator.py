import sqlite3

def calculate_fleet_metrics():
    db_path = "afaos_audit.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    query = """
    SELECT 
        workflow_id,
        COUNT(id) as total_nodes,
        SUM(CASE WHEN completion_status = 'SUCCESS' THEN 1 ELSE 0 END) as direct_successes,
        SUM(CASE WHEN completion_status LIKE '%FALLBACK%' THEN 1 ELSE 0 END) as fallback_successes,
        SUM(CASE WHEN completion_status = 'FAILED' THEN 1 ELSE 0 END) as failures
    FROM execution_audit_logs
    GROUP BY workflow_id;
    """
    
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        
        print("\n📊 --- Day 17 Cross-Node Agent Fleet Aggregations ---")
        for row in rows:
            w_id, total, direct, fallback, fail = row
            success_rate = ((direct + fallback) / total) * 100 if total > 0 else 0
            print(f"\nWorkflow Session: {w_id}")
            print(f" ├─ Total Nodes Processed: {total}")
            print(f" ├─ Success Rate: {success_rate:.1f}%")
            print(f" ├─ Direct Success Nodes: {direct}")
            print(f" └─ Fallback Trigger Events: {fallback}")
            
    except sqlite3.OperationalError as e:
        print(f"❌ Database error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    calculate_fleet_metrics()
