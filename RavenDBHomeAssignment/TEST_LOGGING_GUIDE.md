# Test Logging Output Example

When you run `dotnet test`, the output will look like this:

## Sample Test Run Output

```
═══════════════════════════════════════════════════════════
TEST SETUP: Initializing 3-node cache cluster
═══════════════════════════════════════════════════════════
► Creating nodes...
✓ Nodes created
► Starting Primary on port 5000...
✓ Primary started
► Starting Replica B on port 5001...
✓ Replica B started
► Starting Replica C on port 5002...
✓ Replica C started
► Waiting for cluster stabilization (100ms)...
✓ Cluster stabilized
► Connecting clients to nodes...
  ✓ Client connected to Primary (port 5000)
  ✓ Client connected to Replica B (port 5001)
  ✓ Client connected to Replica C (port 5002)
✅ TEST SETUP COMPLETE - Cluster ready for testing


>>> TEST: HappyPath_WritePrimary_PropagatesToReplicas
────────────────────────────────────────────────
1️⃣  Writing to Primary: SET test_key = test_value
   ✓ Write successful, Primary LSN = 1
2️⃣  Waiting 200ms for replication to propagate to Replicas...
   ✓ Replication wait complete
3️⃣  Reading from Replica B: GET test_key
   ✓ Read from Replica B: test_value
   → Replica B LSN: 1
4️⃣  Reading from Replica C: GET test_key
   ✓ Read from Replica C: test_value
   → Replica C LSN: 1
5️⃣  Asserting consistency...
   ✅ PASS: Replicas converged to same value


>>> TEST: ReadYourWrites_ClientWaitsForMinLsnOnReplica
────────────────────────────────────────────────
1️⃣  Writing to Primary: SET ryw_key = ryw_value
   ✓ Write successful, Primary LSN = 2
2️⃣  Immediately reading from Replica B with MinLsn=2
   → Client will block until Replica B's LSN >= 2
   ✓ Read complete in 45ms
   → Replica B returned: ryw_value
   → Replica B final LSN: 2
3️⃣  Asserting read-your-writes guarantee...
   ✅ PASS: Client saw its own write on Replica (read-your-writes enforced)


>>> TEST: Convergence_DuplicateAndOutOfOrderLsn_IsIdempotent
────────────────────────────────────────────────
1️⃣  First write to Primary: SET idempotent_key = value_one
   ✓ LSN = 3
2️⃣  Second write to Primary: SET idempotent_key = value_two
   ✓ LSN = 4
3️⃣  Validating LSN ordering: 4 > 3
   ✓ LSNs are monotonically increasing
4️⃣  Waiting 200ms for replication...
   ✓ Replication complete
5️⃣  Reading from Replica B: GET idempotent_key
   ✓ Read value: value_two
   → Replica B LSN: 4
6️⃣  Asserting idempotency: value should be latest (value_two)
   ✅ PASS: Despite multiple writes, Replica has correct final value (idempotency enforced)


>>> TEST: Backpressure_MultipleWritesPropagate
────────────────────────────────────────────────
1️⃣  Writing 10 values rapidly to Primary...
   → Written 5/10 entries (LSN = 10)
   → Written 10/10 entries (LSN = 15)
   ✓ All 10 writes complete in 87ms
   → Primary LSN range: 1..15
2️⃣  Waiting 500ms for replication backlog to clear from Replicas...
   ✓ Replication delay complete
   → Replica B final LSN: 15
   → Replica C final LSN: 15
3️⃣  Verifying all 10 entries propagated to Replicas...
   → Verified 5/10 entries on both Replicas
   → Verified 10/10 entries on both Replicas
   ✓ All 10 entries verified on both Replicas
   ✅ PASS: Backpressure mechanism handled bulk writes correctly


═══════════════════════════════════════════════════════════
TEST TEARDOWN: Disposing 3-node cluster
═══════════════════════════════════════════════════════════
► Disconnecting Primary client...
✓ Primary client disconnected
► Disconnecting Replica B client...
✓ Replica B client disconnected
► Disconnecting Replica C client...
✓ Replica C client disconnected
► Shutting down Primary node...
✓ Primary node shutdown
► Shutting down Replica B node...
✓ Replica B node shutdown
► Shutting down Replica C node...
✓ Replica C node shutdown
✅ TEST TEARDOWN COMPLETE
```

## Key Logging Features

### 1. **Timestamps** 
Every log line is prefixed with a precise timestamp (HH:mm:ss.fff) for performance analysis:
```
[12:01:45.123] 1️⃣  Writing to Primary: SET test_key = test_value
[12:01:45.156] ✓ Write successful, Primary LSN = 1
```

### 2. **Visual Progress Indicators**
- `═══` — Section dividers
- `1️⃣ 2️⃣ 3️⃣` — Step numbers
- `►` — Starting action
- `✓` — Success
- `✅` — Test pass
- `→` — Details/metadata

### 3. **LSN Tracking**
All tests log the LSN values to verify monotonic ordering:
```
✓ Write successful, Primary LSN = 1
→ Replica B LSN: 1
→ Replica C LSN: 1
```

### 4. **Performance Metrics**
Read-your-writes test shows actual latency:
```
✓ Read complete in 45ms
→ Replica B returned: ryw_value
```

### 5. **Bulk Operation Progress**
Backpressure test shows incremental progress:
```
→ Written 5/10 entries (LSN = 10)
→ Written 10/10 entries (LSN = 15)
→ Verified 5/10 entries on both Replicas
```

## Running Tests with Logging

```bash
# Run all tests (verbose output)
dotnet test --verbosity detailed --logger "console;verbosity=detailed"

# Run single test
dotnet test --filter "ReadYourWrites"

# Run with minimal output (only test names and results)
dotnet test

# Capture output to file
dotnet test > test-results.log 2>&1
```

## What the Logging Demonstrates

✅ **Cluster Initialization** — Shows each node starting in sequence
✅ **Write Propagation** — Tracks SET from Primary through replication to Replicas
✅ **Read-Your-Writes** — Shows actual ms latency for Replica to catch up
✅ **LSN Ordering** — Verifies monotonic sequence number assignment
✅ **Replication Delays** — Documents actual propagation times
✅ **Backpressure Handling** — Shows bulk writes without blocking
✅ **Convergence** — Confirms all Replicas reach same final state
✅ **Graceful Shutdown** — Demonstrates orderly cluster teardown
