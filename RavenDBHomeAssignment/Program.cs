using RavenDBHomeAssignment.Server;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

Console.WriteLine("RavenDB Home Assignment - 3-Node Distributed Cache");
Console.WriteLine("====================================================");
Console.WriteLine();
Console.WriteLine("Starting 3-node cache cluster...");

var cts = new CancellationTokenSource();

// Create 3-node cluster: 1 Primary + 2 Replicas
var replicaAddresses = new List<(string, int)>
{
    ("127.0.0.1", 5001),
    ("127.0.0.1", 5002)
};

var primary = new CacheNode(isPrimary: true, port: 5000, replicaAddresses);
var replicaB = new CacheNode(isPrimary: false, port: 5001, replicaAddresses: null);
var replicaC = new CacheNode(isPrimary: false, port: 5002, replicaAddresses: null);

// Start all nodes
await primary.StartAsync(cts.Token);
await replicaB.StartAsync(cts.Token);
await replicaC.StartAsync(cts.Token);

Console.WriteLine("✓ Cluster started successfully!");
Console.WriteLine();
Console.WriteLine("Node A (Primary):  127.0.0.1:5000 (accepts writes)");
Console.WriteLine("Node B (Replica):  127.0.0.1:5001 (read-only)");
Console.WriteLine("Node C (Replica):  127.0.0.1:5002 (read-only)");
Console.WriteLine();
Console.WriteLine("To run integration tests:");
Console.WriteLine("  dotnet test");
Console.WriteLine();
Console.WriteLine("Press Ctrl+C to shutdown...");
Console.WriteLine();

// Keep cluster running
try
{
    await Task.Delay(Timeout.Infinite, cts.Token);
}
catch (OperationCanceledException)
{
    Console.WriteLine();
    Console.WriteLine("Shutting down...");
}
finally
{
    await primary.DisposeAsync();
    await replicaB.DisposeAsync();
    await replicaC.DisposeAsync();
    Console.WriteLine("✓ Cluster shutdown complete.");
}

