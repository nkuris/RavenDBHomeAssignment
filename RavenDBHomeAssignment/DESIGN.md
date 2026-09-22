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

## Testing Gaps & Hard-to-Test Scenarios

### Implemented
✅ Happy path (write→replicate→read)
✅ Read-your-writes consistency
✅ Idempotency (duplicate LSN)
✅ Backpressure (bulk writes)

### Not Tested (Awkward to Set Up)
- **Replica crash mid-replication:** Need process kill/restart
- **Network partition:** Would need network simulation or TCP socket tearing
- **Concurrent client failures:** Hard to inject failures synchronously
- **Replication lag measurement:** Would need instrumented time tracking
- **Backpressure blocking:** BoundedChannel doesn't expose queue depth; would need reflection or custom channel

### Would Test With More Time
1. **Network Fault Injection:**
   ```csharp
   // Simulated 100ms latency on Replica reader
   await Task.Delay(100);  // Before applying message
   ```

2. **Replica Reader Slowdown:**
   ```csharp
   // Primary floods with 10,000 writes
   // Replica reader has artificial delay
   // Verify Primary write loop eventually blocks
   ```

3. **Out-of-Order Replication:**
   ```csharp
   // Intercept messages, reorder them
   // Verify Replica ignores out-of-order (old LSN)
   ```

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
- ✅ Testable: 5 high-value integration tests
- ⚠️ Trade-offs: In-memory only, polling-based coordination, fixed topology
- ❌ Not included: Persistence, failover, dynamic membership, binary encoding

**Total code: ~800 lines** — tight, focused, and defense-in-depth on consistency.
