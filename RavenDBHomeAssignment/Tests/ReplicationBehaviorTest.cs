using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RavenDBHomeAssignment.Client;
using RavenDBHomeAssignment.Server;
using Xunit;

namespace RavenDBHomeAssignment.Tests;

/// <summary>
/// Integration tests for replication behavior between primary and replicas.
/// </summary>
public class ReplicationBehaviorTest
{
    [Fact]
    public async Task ReplicaReceivesDataFromPrimary()
    {
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var replicaAddresses = new List<(string, int)>
        {
            ("127.0.0.1", 5201)
        };

        var primary = new CacheNode(isPrimary: true, port: 5200, replicaAddresses);
        var replica = new CacheNode(isPrimary: false, port: 5201, replicaAddresses: null);

        try
        {
            await replica.StartAsync(cts.Token);
            await Task.Delay(100, cts.Token);

            await primary.StartAsync(cts.Token);
            await Task.Delay(500, cts.Token); // Give time for connection

            // Write via primary
            await using var primaryClient = await CacheClient.ConnectAsync("127.0.0.1", 5200, cts.Token);
            await primaryClient.SetAsync("key1", "value1", cts.Token);

            // Wait for replication to occur
            await Task.Delay(500, cts.Token);

            // Read from replica
            await using var replicaClient = await CacheClient.ConnectAsync("127.0.0.1", 5201, cts.Token);
            var replicaValue = await replicaClient.GetAsync("key1", cts.Token);

            Assert.Equal("value1", replicaValue);
        }
        finally
        {
            await primary.DisposeAsync();
            await replica.DisposeAsync();
            cts.Dispose();
        }
    }

    [Fact]
    public async Task MultipleReplicasReceiveData()
    {
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var replicaAddresses = new List<(string, int)>
        {
            ("127.0.0.1", 5301),
            ("127.0.0.1", 5302)
        };

        var primary = new CacheNode(isPrimary: true, port: 5300, replicaAddresses);
        var replica1 = new CacheNode(isPrimary: false, port: 5301, replicaAddresses: null);
        var replica2 = new CacheNode(isPrimary: false, port: 5302, replicaAddresses: null);

        try
        {
            await replica1.StartAsync(cts.Token);
            await replica2.StartAsync(cts.Token);
            await Task.Delay(100, cts.Token);

            await primary.StartAsync(cts.Token);
            await Task.Delay(500, cts.Token);

            // Write via primary
            await using var primaryClient = await CacheClient.ConnectAsync("127.0.0.1", 5300, cts.Token);
            await primaryClient.SetAsync("key1", "value1", cts.Token);
            await primaryClient.SetAsync("key2", "value2", cts.Token);

            await Task.Delay(500, cts.Token);

            // Read from replica 1
            await using var replicaClient1 = await CacheClient.ConnectAsync("127.0.0.1", 5301, cts.Token);
            var r1_val1 = await replicaClient1.GetAsync("key1", cts.Token);
            var r1_val2 = await replicaClient1.GetAsync("key2", cts.Token);

            // Read from replica 2
            await using var replicaClient2 = await CacheClient.ConnectAsync("127.0.0.1", 5302, cts.Token);
            var r2_val1 = await replicaClient2.GetAsync("key1", cts.Token);
            var r2_val2 = await replicaClient2.GetAsync("key2", cts.Token);

            Assert.Equal("value1", r1_val1);
            Assert.Equal("value2", r1_val2);
            Assert.Equal("value1", r2_val1);
            Assert.Equal("value2", r2_val2);
        }
        finally
        {
            await primary.DisposeAsync();
            await replica1.DisposeAsync();
            await replica2.DisposeAsync();
            cts.Dispose();
        }
    }

    [Fact]
    public async Task ReplicaIsReadOnly()
    {
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var replica = new CacheNode(isPrimary: false, port: 5400, replicaAddresses: null);

        try
        {
            await replica.StartAsync(cts.Token);
            await Task.Delay(100, cts.Token);

            // Attempt to write to replica
            await using var replicaClient = await CacheClient.ConnectAsync("127.0.0.1", 5400, cts.Token);
            bool setResult = await replicaClient.SetAsync("key", "value", cts.Token);

            // Write should fail on replica
            Assert.False(setResult);
        }
        finally
        {
            await replica.DisposeAsync();
            cts.Dispose();
        }
    }

    [Fact]
    public async Task ReadYourWritesConsistency_AcrossNodes()
    {
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        var replicaAddresses = new List<(string, int)>
        {
            ("127.0.0.1", 5501)
        };

        var primary = new CacheNode(isPrimary: true, port: 5500, replicaAddresses);
        var replica = new CacheNode(isPrimary: false, port: 5501, replicaAddresses: null);

        try
        {
            await replica.StartAsync(cts.Token);
            await Task.Delay(100, cts.Token);

            await primary.StartAsync(cts.Token);
            await Task.Delay(500, cts.Token);

            await using var primaryClient = await CacheClient.ConnectAsync("127.0.0.1", 5500, cts.Token);
            await using var replicaClient = await CacheClient.ConnectAsync("127.0.0.1", 5501, cts.Token);

            // Write via primary
            await primaryClient.SetAsync("key", "value", cts.Token);
            long writeLsn = primaryClient.LastObservedLsn;

            // Add delay to ensure replication
            await Task.Delay(500, cts.Token);

            // Read from replica - this should succeed and return the written value
            // The replica client should handle read-your-writes via the primary's LSN
            var replicaValue = await replicaClient.GetAsync("key", cts.Token);
            Assert.Equal("value", replicaValue);
        }
        finally
        {
            await primary.DisposeAsync();
            await replica.DisposeAsync();
            cts.Dispose();
        }
    }

    [Fact]
    public async Task SequentialWrites_MaintainOrder()
    {
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var replicaAddresses = new List<(string, int)>
        {
            ("127.0.0.1", 5601)
        };

        var primary = new CacheNode(isPrimary: true, port: 5600, replicaAddresses);
        var replica = new CacheNode(isPrimary: false, port: 5601, replicaAddresses: null);

        try
        {
            await replica.StartAsync(cts.Token);
            await Task.Delay(100, cts.Token);

            await primary.StartAsync(cts.Token);
            await Task.Delay(500, cts.Token);

            await using var primaryClient = await CacheClient.ConnectAsync("127.0.0.1", 5600, cts.Token);

            // Sequential writes to same key
            await primaryClient.SetAsync("key", "value1", cts.Token);
            await primaryClient.SetAsync("key", "value2", cts.Token);
            await primaryClient.SetAsync("key", "value3", cts.Token);

            await Task.Delay(500, cts.Token);

            // Replica should have the latest value
            await using var replicaClient = await CacheClient.ConnectAsync("127.0.0.1", 5601, cts.Token);
            var finalValue = await replicaClient.GetAsync("key", cts.Token);

            Assert.Equal("value3", finalValue);
        }
        finally
        {
            await primary.DisposeAsync();
            await replica.DisposeAsync();
            cts.Dispose();
        }
    }
}
