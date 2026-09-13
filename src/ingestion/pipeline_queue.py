import time
from collections import deque
from typing import Dict, Any, Optional

class FinancialPipelineQueue:
    """High-performance bounded queue acting as a memory shock-absorber for incoming streams."""
    
    def __init__(self, max_buffer_size: int = 500):
        # Initialize an in-memory double-ended queue with strict sizing boundaries
        self.buffer = deque(maxlen=max_buffer_size)
        print(f"⚡ [QUEUE INITIALIZED]: In-memory streaming buffer locked at max capacity: {max_buffer_size}")

    def push_transaction(self, tx_payload: Dict[str, Any]) -> bool:
        """Pushes an incoming raw ledger transaction safely into the memory buffer."""
        if not tx_payload.get("txn_id") or not tx_payload.get("amount"):
            print("⚠️ [QUEUE REJECTION]: Dropped corrupt payload chunk — missing key financial attributes.")
            return False
            
        self.buffer.append(tx_payload)
        return True

    def pop_batch(self, batch_size: int = 100) -> list:
        """Pops a clean chunk of transaction logs for batch processing out of the buffer queue."""
        batch = []
        while self.buffer and len(batch) < batch_size:
            batch.append(self.buffer.popleft()) # Fast O(1) left-side removal primitive
        return batch

    def get_current_load(self) -> int:
        """Returns the active number of tracked elements residing inside RAM memory."""
        return len(self.buffer)

if __name__ == "__main__":
    # Test execution harness with a strict constraint of 5 elements maximum
    queue_harness = FinancialPipelineQueue(max_buffer_size=5)
    
    # Simulate a high-speed micro-burst payload pushing 6 items (TXN_101 to TXN_106)
    print("\n🚀 Simulating stream ingestion burst data inflow...")
    for i in range(1, 7):
        queue_harness.push_transaction({"txn_id": f"TXN_{100+i}", "amount": 250.50 * i})
        
    print(f"📊 Active load sitting inside queue RAM: {queue_harness.get_current_load()} elements.")
