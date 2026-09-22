using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RavenDBHomeAssignment.Server;
using Xunit;

namespace RavenDBHomeAssignment.Tests;

/// <summary>
/// Simple test to debug replication connection establishment.
/// </summary>
public class ReplicationConnectionTest
{
    [Fact]
    public async Task PrimaryConnectsToReplicaReplicationPorts()
    {
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var replicaAddresses = new List<(string, int)>
        {
            ("127.0.0.1", 5001),
            ("127.0.0.1", 5002)
        };

        Console.WriteLine("Creating nodes...");
        var primary = new CacheNode(isPrimary: true, port: 5000, replicaAddresses);
        var replicaB = new CacheNode(isPrimary: false, port: 5001, replicaAddresses: null);
        var replicaC = new CacheNode(isPrimary: false, port: 5002, replicaAddresses: null);

        try
        {
            Console.WriteLine("Starting Replica B...");
            await replicaB.StartAsync(cts.Token);
            Console.WriteLine("✓ Replica B started");

            Console.WriteLine("Starting Replica C...");
            await replicaC.StartAsync(cts.Token);
            Console.WriteLine("✓ Replica C started");

            Console.WriteLine("Starting Primary...");
            await primary.StartAsync(cts.Token);
            Console.WriteLine("✓ Primary started");

            Console.WriteLine("Waiting 1 second for replication connections...");
            await Task.Delay(1000, cts.Token);

            Console.WriteLine($"Primary connected replicas: {primary.ConnectedReplicaCount}");
            Assert.Equal(2, primary.ConnectedReplicaCount);

            Console.WriteLine("✓ Test passed!");
        }
        finally
        {
            Console.WriteLine("Cleaning up...");
            await primary.DisposeAsync();
            await replicaB.DisposeAsync();
            await replicaC.DisposeAsync();
            cts.Dispose();
        }
    }
}
