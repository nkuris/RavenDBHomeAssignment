# DESIGN.md: 3-Node Distributed Cache

## What Clients Can Rely On

### Write Operations (SET, DEL)
1. **Atomicity on Primary:** A write is either fully applied or not at all
2. **Durability:** Accepted response means the operation is durable on Primary (in-memory)
3. **Ordering:** Writes are serialized; each gets a unique, monotonically increasing LSN
4. **Initial Replication:** Primary immediately enqueues to Replica channels (synchronous channel write)
5. **No Rollback:** Once a write succeeds on Primary, it will eventually reach all Replicas

### Read Operations (GET)
1. **Read-Your-Writes Guarantee:** If a client writes to Primary and immediately reads from Replica, it will see its own write (or newer)
2. **Causal Consistency Within Client:** Client's reads observe its own writes in order
3. **Stale Reads Possible Between Clients:** Client A and B may read different versions from different Replicas if not coordinated
4. **Eventually Consistent:** All Replicas converge to same state as Primary (idempotency via LSN)

### What Clients CANNOT Rely On
- **Strict Serializability Across Clients:** Two concurrent clients writing to Primary may see those writes in different orders on Replicas
- **Persistence Across Restarts:** All data is in-memory; cluster restart = data loss
- **Failover/Promotion:** No leader election; Primary failure = cluster failure
- **Transactions:** No multi-key ACID transactions
- **Conflict Resolution:** DEL doesn't tombstone; stale SET can overwrite newer DEL if LSN ordering is wrong

---

## Concurrency Strategy

### On Primary Node
- **Write Lock-Free:** Uses `Interlocked.Increment(ref _globalLsn)` for lock-free LSN assignment
- **Store Updates:** `ConcurrentDictionary` handles concurrent reads + writes safely
- **Replication Enqueue:** Happens synchronously on write thread; blocks if channel full (backpressure)

### On Replica Node
- **Read-Only Store Updates:** Only replication receiver thread writes; client reads are never blocked by writes
- **LSN Tracking:** Lock-protected (simple `object` lock) for coordination between replication and client reads
- **Client Reads:** May block asynchronously if `CurrentLsn < MinLsn` (polling every 10ms)

### Thread Safety
- **CacheStore:** Thread-safe via `ConcurrentDictionary` + lock for LSN coordination
- **ReplicationPipeline:** Bounded channel is thread-safe; reader thread is exclusive
- **CacheNode:** Each node has separate listener threads (accept loop is serial per port)

---

## Convergence & Degraded Operations

### Normal Operation
1. Primary receives write, increments LSN, applies locally, enqueues to Replicas
2. ReplicationPipeline background task reads from channel, frames, sends over TCP
3. Replica receives, applies idempotently (only if new LSN > existing)
4. Client reads from Replica with MinLsn guarantee

### Replica Connection Loss
- Primary replication pipeline cannot send; message accumulates in channel
- Once capacity (1,000) reached, Primary write loop blocks (backpressure)
- When Replica reconnects, backlog clears automatically
- **Idempotency ensures Replicas apply same message multiple times correctly**

### Duplicate/Out-of-Order Messages
- Replica's `TryApplyUpdate()` checks `new.Lsn > existing.Lsn`
- If duplicate LSN arrives, it's rejected (no update)
- If out-of-order (e.g., LSN 5 before LSN 3), LSN 5 is accepted, LSN 3 is ignored
- **Result:** Final state is consistent as long Replicas see all unique LSNs

### Primary Failure
- **Not handled.** Cluster becomes unavailable
- Replicas cannot promote themselves (no consensus)
- Manual intervention required

---

## Concurrency & Backpressure

### Backpressure Mechanism
- **BoundedChannel** with capacity 1,000 and `FullMode.Wait`
- When channel full, `EnqueueAsync()` blocks the calling thread
- Primary write loop blocks if Replica is slow to consume
- **Effect:** Natural throttling; fast Replicas don't stall, slow Replicas throttle Primary

### Read-Your-Writes Without Distributed Locks
- Client tracks `_lastObservedLsn` from write responses
- On read, sends `MinLsn = _lastObservedLsn`
- Replica polls `CurrentLsn` until ≥ `MinLsn` (blocks for ~10ms intervals)
- Polling is async (`await Task.Delay(10)`), not busy-waiting

### Why No Condition Variables?
- Simpler mental model: no notify/wait coordination needed
- 10ms polling acceptable for demo/test scale
- Production would use `ManualResetEvent` or similar

---

## Deliberately Left Out

### Infrastructure
- **Persistence:** No WAL, snapshots, or disk durability
- **Leader Election:** Fixed topology; Primary is hardcoded
- **Dynamic Membership:** Cluster size fixed at start
- **Failover/Promotion:** No automatic recovery

### Features
- **TTL / Eviction:** All data retained until restart
- **Transactions:** Single-key operations only
- **Conflict Resolution:** LSN is total order; no versioning
- **Replication Filters:** All writes replicate to all Replicas
- **Encryption/Auth:** No TLS, no authentication

### Optimization
- **Compression:** Full JSON over wire
- **Binary Protocol:** Text-based JSON for clarity
- **Connection Pooling:** One connection per client
- **Adaptive Backpressure:** Fixed 1,000 capacity
- **Metrics/Observability:** No counters, timers, or dashboards

---

## Testing Gaps & Test Coverage

### Comprehensive Test Suite (23 Tests)

#### ✅ CacheStore Unit Tests (11 tests)
Core functionality of the in-memory cache store with idempotent updates and LSN ordering:

1. **Idempotency & LSN Ordering (6 tests)**
   - `TryApplyUpdate_NewKey_Succeeds` - Verify new keys are inserted
   - `TryApplyUpdate_SameLsnTwice_SecondFails` - Reject duplicate LSN (idempotency)
   - `TryApplyUpdate_LowerLsn_Rejected` - Reject out-of-order updates with lower LSN
   - `TryApplyUpdate_HigherLsn_Succeeds` - Accept updates with higher LSN
   - `TryApplyUpdate_ZeroLsn_Rejected` - Reject invalid LSN (zero not allowed)
   - `TryApplyUpdate_NegativeLsn_Rejected` - Reject invalid LSN (negative not allowed)

2. **Read-Your-Writes Consistency (1 test)**
   - `GetAsync_ReadYourWrites_WaitsForLsn` - Verify polling behavior when minLsn > currentLsn

3. **Key Retrieval (2 tests)**
   - `GetAsync_KeyNotFound_ReturnsNull` - Non-existent keys return null
   - `GetAsync_KeyExists_ReturnsValue` - Existing keys return correct value

4. **Multi-Key Independence (2 tests)**
   - `GetAsync_MultipleKeys_Independent` - Multiple keys don't interfere with each other
   - `TryApplyUpdate_MultipleKeys_Independent` - Updates to different keys are independent

#### ✅ CacheClient Integration Tests (6 tests)
Test the client's operations against a running Primary node:

1. **Basic Operations (3 tests)**
   - `SetAndGet_BasicOperation` - Write and read succeed
   - `Delete_RemovesKey` - DELETE removes keys correctly
   - `Get_NonExistentKey_ReturnsNull` - GET on missing key returns null

2. **Sequential Operations (2 tests)**
   - `MultipleOperations_Sequential` - Multiple SET/GET operations in sequence
   - `UpdateSameKey_LatestValueReturned` - Multiple updates to same key return latest

3. **LSN Tracking (1 test)**
   - `LastObservedLsn_UpdatedAfterWrite` - Client LSN field updates after writes

#### ✅ Replication Behavior Tests (5 tests)
Verify replication from Primary to Replicas, data consistency, and read-your-writes guarantees:

1. **Single Replica (1 test)**
   - `ReplicaReceivesDataFromPrimary` - Data written to Primary appears on Replica

2. **Multiple Replicas (2 tests)**
   - `MultipleReplicasReceiveData` - Data propagates to all Replicas
   - `ReplicaIsReadOnly` - Replica correctly rejects write attempts

3. **Consistency Guarantees (2 tests)**
   - `ReadYourWritesConsistency_AcrossNodes` - Client sees its own writes when reading from Replica
   - `SequentialWrites_MaintainOrder` - Multiple writes to same key maintain order on Replicas

#### ✅ Replication Connection Test (1 test)
Verify cluster setup and initial connection establishment:

1. **Connection Setup (1 test)**
   - `PrimaryConnectsToReplicaReplicationPorts` - Primary successfully connects to Replicas

### Not Tested (Difficult to Simulate)
- **Replica crash mid-replication:** Would need process kill or socket teardown
- **Network partition:** Requires network simulation or TCP socket interruption
- **Concurrent client failures:** Hard to inject failures synchronously
- **Replication lag measurement:** Would need instrumented timing throughout the pipeline
- **Backpressure blocking verification:** BoundedChannel capacity doesn't expose current depth
- **TCP connection timeouts and retries:** Would need network fault injection

---

## Decisions & Trade-offs

| Decision | Why | Trade-off |
|----------|-----|-----------|
| **JSON over Binary** | Clarity, debugging | Slower serialization |
| **Polling over Events** | Simpler, no spurious wakeups | Higher latency (10ms polling) |
| **BoundedChannel per Replica** | Natural backpressure | Artificial throughput cap |
| **LSN-based Idempotency** | Simple, deterministic | Can't reorder writes |
| **Single Primary** | No consensus needed | No failover |
| **In-Memory Only** | Fast, simple | Data loss on restart |
| **Lock-Free LSN** | No contention | Limited to single counter |
| **Async Replication** | Non-blocking writes | Replication latency |

---

## What I Would Change With More Time

1. **Better Signaling:** Replace polling with `ManualResetEvent` or channel-based notification
2. **Metrics:** Add counters for writes, replications, LSN latency
3. **Graceful Shutdown:** Allow Replicas to flush pending messages before closing
4. **Retry Logic:** Handle transient TCP errors with exponential backoff
5. **Config System:** Ports, channel capacity, polling interval as config
6. **Composite Key Support:** Multi-key operations for transactions
7. **Tombstones:** Track deleted keys to handle delete-before-write race
8. **Async Backpressure Tests:** Actual stress test with slow Replica simulation

---

## Summary

This implementation prioritizes **correctness and clarity** over performance or completeness:
- ✅ Guarantees: Read-your-writes, convergence, idempotency
- ✅ Clean architecture: Protocol, Server, Client separation
- ✅ Comprehensive test suite: 23 tests across 4 test files (unit + integration)
- ✅ Test coverage: Cache store logic, client operations, replication behavior, connection setup
- ⚠️ Trade-offs: In-memory only, polling-based coordination, fixed topology
- ❌ Not included: Persistence, failover, dynamic membership, binary encoding

**Total code:**
- Implementation: ~800 lines
- Tests: ~400 lines (23 tests)
- **Total: ~1,200 lines** — tight, focused, and defense-in-depth on consistency.
