using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RavenDBHomeAssignment.Client;
using RavenDBHomeAssignment.Server;
using Xunit;

namespace RavenDBHomeAssignment.Tests;

/// <summary>
/// Integration tests for CacheClient operations (SET, GET, DELETE) against a CacheNode.
/// </summary>
public class CacheClientOperationsTest
{
    [Fact]
    public async Task SetAndGet_BasicOperation()
    {
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var node = new CacheNode(isPrimary: true, port: 5100, replicaAddresses: null);

        try
        {
            await node.StartAsync(cts.Token);
            await Task.Delay(100, cts.Token); // Give server time to start

            await using var client = await CacheClient.ConnectAsync("127.0.0.1", 5100, cts.Token);

            // Set a key-value pair
            bool setResult = await client.SetAsync("testkey", "testvalue", cts.Token);
            Assert.True(setResult, "SET operation should succeed");

            // Get the value back
            var getValue = await client.GetAsync("testkey", cts.Token);
            Assert.Equal("testvalue", getValue);
        }
        finally
        {
            await node.DisposeAsync();
            cts.Dispose();
        }
    }

    [Fact]
    public async Task Delete_RemovesKey()
    {
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var node = new CacheNode(isPrimary: true, port: 5101, replicaAddresses: null);

        try
        {
            await node.StartAsync(cts.Token);
            await Task.Delay(100, cts.Token);

            await using var client = await CacheClient.ConnectAsync("127.0.0.1", 5101, cts.Token);

            // Set a value
            await client.SetAsync("deletekey", "deletevalue", cts.Token);

            // Delete the key
            bool deleteResult = await client.DeleteAsync("deletekey", cts.Token);
            Assert.True(deleteResult, "DELETE operation should succeed");

            // Get should return null
            var getValue = await client.GetAsync("deletekey", cts.Token);
            Assert.Null(getValue);
        }
        finally
        {
            await node.DisposeAsync();
            cts.Dispose();
        }
    }

    [Fact]
    public async Task Get_NonExistentKey_ReturnsNull()
    {
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var node = new CacheNode(isPrimary: true, port: 5102, replicaAddresses: null);

        try
        {
            await node.StartAsync(cts.Token);
            await Task.Delay(100, cts.Token);

            await using var client = await CacheClient.ConnectAsync("127.0.0.1", 5102, cts.Token);

            var result = await client.GetAsync("nonexistent", cts.Token);
            Assert.Null(result);
        }
        finally
        {
            await node.DisposeAsync();
            cts.Dispose();
        }
    }

    [Fact]
    public async Task MultipleOperations_Sequential()
    {
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var node = new CacheNode(isPrimary: true, port: 5103, replicaAddresses: null);

        try
        {
            await node.StartAsync(cts.Token);
            await Task.Delay(100, cts.Token);

            await using var client = await CacheClient.ConnectAsync("127.0.0.1", 5103, cts.Token);

            // Set key1
            await client.SetAsync("key1", "value1", cts.Token);

            // Set key2
            await client.SetAsync("key2", "value2", cts.Token);

            // Set key3
            await client.SetAsync("key3", "value3", cts.Token);

            // Verify all values
            var v1 = await client.GetAsync("key1", cts.Token);
            var v2 = await client.GetAsync("key2", cts.Token);
            var v3 = await client.GetAsync("key3", cts.Token);

            Assert.Equal("value1", v1);
            Assert.Equal("value2", v2);
            Assert.Equal("value3", v3);
        }
        finally
        {
            await node.DisposeAsync();
            cts.Dispose();
        }
    }

    [Fact]
    public async Task UpdateSameKey_LatestValueReturned()
    {
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var node = new CacheNode(isPrimary: true, port: 5104, replicaAddresses: null);

        try
        {
            await node.StartAsync(cts.Token);
            await Task.Delay(100, cts.Token);

            await using var client = await CacheClient.ConnectAsync("127.0.0.1", 5104, cts.Token);

            // Set key to value1
            await client.SetAsync("key", "value1", cts.Token);
            var result1 = await client.GetAsync("key", cts.Token);
            Assert.Equal("value1", result1);

            // Update key to value2
            await client.SetAsync("key", "value2", cts.Token);
            var result2 = await client.GetAsync("key", cts.Token);
            Assert.Equal("value2", result2);

            // Update key to value3
            await client.SetAsync("key", "value3", cts.Token);
            var result3 = await client.GetAsync("key", cts.Token);
            Assert.Equal("value3", result3);
        }
        finally
        {
            await node.DisposeAsync();
            cts.Dispose();
        }
    }

    [Fact]
    public async Task LastObservedLsn_UpdatedAfterWrite()
    {
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var node = new CacheNode(isPrimary: true, port: 5105, replicaAddresses: null);

        try
        {
            await node.StartAsync(cts.Token);
            await Task.Delay(100, cts.Token);

            await using var client = await CacheClient.ConnectAsync("127.0.0.1", 5105, cts.Token);

            // Initially LSN should be 0
            Assert.Equal(0, client.LastObservedLsn);

            // Set a value
            await client.SetAsync("key1", "value1", cts.Token);
            long lsn1 = client.LastObservedLsn;
            Assert.True(lsn1 > 0, "LSN should be updated after SET");

            // Set another value
            await client.SetAsync("key2", "value2", cts.Token);
            long lsn2 = client.LastObservedLsn;
            Assert.True(lsn2 >= lsn1, "LSN should not decrease");
        }
        finally
        {
            await node.DisposeAsync();
            cts.Dispose();
        }
    }
}
