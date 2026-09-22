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

```bash
dotnet test
```

### Test Scenarios
- **HappyPath_WritePrimary_PropagatesToReplicas:** Write on Primary, verify both Replicas receive it
- **ReadYourWrites_ClientWaitsForMinLsnOnReplica:** Write→Primary, immediately read→Replica, verify consistency
- **Convergence_DuplicateAndOutOfOrderLsn_IsIdempotent:** Send duplicate writes, verify only applied once
- **Backpressure_MultipleWritesPropagate:** Rapid writes to Primary, verify all propagate to Replicas

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
	└── CacheClusterTests.cs     # xUnit integration tests
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

Tests use `IAsyncLifetime` to spin up a full 3-node cluster for each test class, then tear down. Each test is ~100-500ms due to replication latency and polling delays.

### Timeout Configuration
- Test timeout: 30 seconds (via `CancellationTokenSource`)
- Replication wait: 200ms (empirical; accounts for async pipeline + store polling)

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

Total: ~800 lines (protocol + server + client + tests)
