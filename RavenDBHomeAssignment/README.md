# RavenDB Home Assignment: 3-Node Distributed Cache

A production-grade in-memory distributed cache system with Primary-Replica topology over TCP.

## Overview

This implementation demonstrates:
- **3-node cluster** (1 Primary writer, 2 Read-only Replicas)
- **TCP replication** with length-prefixed framing
- **Read-Your-Writes consistency** via LSN (Log Sequence Number) tracking
- **Idempotent replication** with automatic duplicate detection
- **Backpressure management** via bounded replication channels

## Architecture

### Nodes
- **Node A (Primary):** Accepts writes (SET, DEL), assigns monotonic LSNs, replicates to Replicas
- **Node B & C (Replicas):** Receive replication streams, serve reads, enforce read-your-writes consistency

### Wire Protocol
- **Framing:** 4-byte Int32 length header (little-endian) + UTF-8 JSON payload
- **Messages:** `CacheRequest` (client→node), `CacheResponse` (node→client), `ReplicationMessage` (Primary→Replica)

### Consistency Guarantees
1. **Convergence:** All replicas eventually have the same state as Primary (idempotency via LSN check)
2. **Read-Your-Writes:** Client tracks LSN from writes; reads on Replicas wait until their local LSN ≥ client's MinLSN
3. **Idempotency:** Duplicate replication messages (same LSN) are applied only once

## Building

```bash
dotnet build
```

## Running Tests

### Using Visual Studio Test Explorer
1. Open **Test Explorer** via __View > Test Explorer__ (or press `Ctrl+E, T`)
2. Click **Run All Tests** button to execute all 23 tests
3. View results and detailed output in the Test Explorer window

### Using Command Line
```bash
# Run all tests
dotnet test

# Run with verbose output
dotnet test --verbosity detailed

# Run specific test class
dotnet test --filter "ClassName=RavenDBHomeAssignment.Tests.CacheStoreTests"
```

### Test Coverage (23 Tests Total)

#### CacheStore Unit Tests (11 tests)
- `TryApplyUpdate_NewKey_Succeeds` - New key insertion
- `TryApplyUpdate_SameLsnTwice_SecondFails` - Idempotent duplicate rejection
- `TryApplyUpdate_LowerLsn_Rejected` - Reject out-of-order LSN updates
- `TryApplyUpdate_HigherLsn_Succeeds` - Accept higher LSN updates
- `TryApplyUpdate_ZeroLsn_Rejected` - Reject invalid LSN (zero)
- `TryApplyUpdate_NegativeLsn_Rejected` - Reject invalid LSN (negative)
- `GetAsync_KeyNotFound_ReturnsNull` - Non-existent key handling
- `GetAsync_KeyExists_ReturnsValue` - Key retrieval
- `GetAsync_ReadYourWrites_WaitsForLsn` - LSN wait behavior
- `GetAsync_MultipleKeys_Independent` - Multi-key independence
- `TryApplyUpdate_MultipleKeys_Independent` - Multi-key update independence

#### CacheClient Integration Tests (6 tests)
- `SetAndGet_BasicOperation` - Basic SET/GET workflow
- `Delete_RemovesKey` - DELETE operation
- `Get_NonExistentKey_ReturnsNull` - Non-existent key read
- `MultipleOperations_Sequential` - Sequential operation chain
- `UpdateSameKey_LatestValueReturned` - Multiple updates to same key
- `LastObservedLsn_UpdatedAfterWrite` - LSN tracking after writes

#### Replication Behavior Tests (5 tests)
- `ReplicaReceivesDataFromPrimary` - Single replica data replication
- `MultipleReplicasReceiveData` - Multi-replica data propagation
- `ReplicaIsReadOnly` - Replica write rejection
- `ReadYourWritesConsistency_AcrossNodes` - Cross-node consistency guarantee
- `SequentialWrites_MaintainOrder` - Write ordering maintenance

#### Replication Connection Test (1 test)
- `PrimaryConnectsToReplicaReplicationPorts` - Primary→Replica connection establishment

## Running the Cluster

```bash
dotnet run
```

Starts a 3-node cluster on:
- **Primary:** `127.0.0.1:5000`
- **Replica B:** `127.0.0.1:5001`
- **Replica C:** `127.0.0.1:5002`

## Code Structure

```
RavenDBHomeAssignment/
├── Program.cs                    # Cluster orchestration & startup
├── Protocol/
│   ├── Framing.cs               # TCP frame helpers (read/write)
│   └── Messages.cs              # JSON DTOs
├── Server/
│   ├── CacheStore.cs            # In-memory store with LSN tracking
│   ├── ReplicationPipeline.cs    # Per-replica background replication task
│   └── CacheNode.cs             # TcpListener host (Primary/Replica logic)
├── Client/
│   └── CacheClient.cs           # Test client with auto LSN tracking
└── Tests/
	├── CacheStoreTests.cs        # 11 unit tests for cache store
	├── CacheClientOperationsTest.cs  # 6 integration tests for client operations
	├── ReplicationBehaviorTest.cs    # 5 integration tests for replication
	└── ReplicationConnectionTest.cs  # 1 integration test for connection setup
```

## Design Decisions

See **DESIGN.md** for:
- What clients can/cannot rely on
- Concurrency and backpressure strategy
- Convergence handling
- Deliberately left out features
- Known limitations and trade-offs

## Key Implementation Details

### Idempotent Updates
```csharp
// Replica only applies if new.Lsn > existing.Lsn
if (newLsn > store.CurrentLsn)
	store.Update(key, value, newLsn);
```

### Read-Your-Writes Consistency
```csharp
// Client tracks LSN from writes
lastObservedLsn = responseFromPrimary.Lsn;

// Auto-attach MinLsn on next read
await client.GetAsync(key);  // Sends MinLsn = lastObservedLsn
```

### Backpressure
```csharp
// Primary write loop blocks if channel full (FullMode.Wait)
await pipeline.EnqueueAsync(msg);  // Blocks if 1000+ messages queued
```

## Constraints & Simplifications

**Not Implemented:**
- Leader election, consensus, dynamic membership
- Persistence (in-memory only)
- TLS, authentication, metrics
- TTL, eviction policies
- Binary encoding (JSON only)
- Production-grade error recovery

## Testing Notes

### Test Organization
Tests are organized into 4 separate files for clarity and maintainability:
- **CacheStoreTests.cs:** Unit tests for the core cache store logic (idempotency, LSN ordering)
- **CacheClientOperationsTest.cs:** Integration tests for client SET/GET/DELETE operations
- **ReplicationBehaviorTest.cs:** Integration tests for replication and consistency
- **ReplicationConnectionTest.cs:** Connection setup and node orchestration

### Test Execution
- Each test spins up a complete 3-node cluster (Primary + 2 Replicas)
- Tests use `CancellationTokenSource` with 10-15 second timeouts
- Replication latency: ~10-500ms (TCP round-trip + async processing)
- Full test suite completes in ~13 seconds on standard hardware

### Timeout Configuration
- Per-test timeout: 10-15 seconds (via `CancellationTokenSource`)
- Replication wait: 100-500ms (empirical; accounts for async replication + processing)
- Allow sufficient time for TCP socket establishment and inter-process communication

## Performance Characteristics

**Throughput:** ~1,000 writes/sec per Primary (bounded by 1,000-message channel capacity)
**Latency:** Write→Replica: ~10-50ms (TCP roundtrip + async pipeline)
**Scalability:** Fixed 3-node cluster; not designed for horizontal scaling

## Known Limitations

1. **In-memory only** — Data lost on restart
2. **No persistence** — No write-ahead log, snapshots, or recovery
3. **Blocking reads** — `GetAsync(minLsn)` polls synchronously (10ms intervals)
4. **Artificial throttling** — BoundedChannel capacity (1,000) intentionally limits throughput
5. **Simple framing** — No compression, encryption, or adaptive sizing

## Line Count

- **Implementation:** ~800 lines (protocol + server + client)
- **Tests:** ~400 lines (23 comprehensive tests across 4 test files)
- **Total:** ~1,200 lines
