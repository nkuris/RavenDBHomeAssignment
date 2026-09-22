using System.Threading;
using System.Threading.Tasks;
using RavenDBHomeAssignment.Server;
using Xunit;

namespace RavenDBHomeAssignment.Tests;

/// <summary>
/// Unit tests for CacheStore idempotent update and read-your-writes consistency.
/// </summary>
public class CacheStoreTests
{
    [Fact]
    public void TryApplyUpdate_NewKey_Succeeds()
    {
        var store = new CacheStore();
        bool result = store.TryApplyUpdate("key1", "value1", 1);
        Assert.True(result);
    }

    [Fact]
    public void TryApplyUpdate_SameLsnTwice_SecondFails()
    {
        var store = new CacheStore();
        bool first = store.TryApplyUpdate("key1", "value1", 1);
        bool second = store.TryApplyUpdate("key1", "value2", 1);

        Assert.True(first);
        Assert.False(second); // Same LSN should be rejected
    }

    [Fact]
    public void TryApplyUpdate_LowerLsn_Rejected()
    {
        var store = new CacheStore();
        store.TryApplyUpdate("key1", "value1", 5);
        bool result = store.TryApplyUpdate("key1", "value2", 3);

        Assert.False(result);
    }

    [Fact]
    public void TryApplyUpdate_HigherLsn_Succeeds()
    {
        var store = new CacheStore();
        store.TryApplyUpdate("key1", "value1", 1);
        bool result = store.TryApplyUpdate("key1", "value2", 2);

        Assert.True(result);
    }

    [Fact]
    public void TryApplyUpdate_ZeroLsn_Rejected()
    {
        var store = new CacheStore();
        bool result = store.TryApplyUpdate("key1", "value1", 0);

        Assert.False(result);
    }

    [Fact]
    public void TryApplyUpdate_NegativeLsn_Rejected()
    {
        var store = new CacheStore();
        bool result = store.TryApplyUpdate("key1", "value1", -1);

        Assert.False(result);
    }

    [Fact]
    public async Task GetAsync_KeyNotFound_ReturnsNull()
    {
        var store = new CacheStore();
        var result = await store.GetAsync("nonexistent", 0, CancellationToken.None);

        Assert.Null(result);
    }

    [Fact]
    public async Task GetAsync_KeyExists_ReturnsValue()
    {
        var store = new CacheStore();
        store.TryApplyUpdate("key1", "value1", 1);

        var result = await store.GetAsync("key1", 0, CancellationToken.None);

        Assert.Equal("value1", result);
    }

    [Fact]
    public async Task GetAsync_ReadYourWrites_WaitsForLsn()
    {
        var store = new CacheStore();
        store.TryApplyUpdate("key1", "value1", 5);

        var cts = new CancellationTokenSource();
        cts.CancelAfter(500); // 500ms timeout

        // This should timeout because LSN 5 hasn't been reached yet
        // TaskCanceledException is thrown, which is a subclass of OperationCanceledException
        var ex = await Assert.ThrowsAsync<TaskCanceledException>(
            () => store.GetAsync("key1", 10, cts.Token)
        );

        Assert.NotNull(ex);
    }

    [Fact]
    public async Task GetAsync_MultipleKeys_Independent()
    {
        var store = new CacheStore();
        store.TryApplyUpdate("key1", "value1", 1);
        store.TryApplyUpdate("key2", "value2", 2);
        store.TryApplyUpdate("key3", "value3", 3);

        var r1 = await store.GetAsync("key1", 0, CancellationToken.None);
        var r2 = await store.GetAsync("key2", 0, CancellationToken.None);
        var r3 = await store.GetAsync("key3", 0, CancellationToken.None);

        Assert.Equal("value1", r1);
        Assert.Equal("value2", r2);
        Assert.Equal("value3", r3);
    }

    [Fact]
    public void TryApplyUpdate_MultipleKeys_Independent()
    {
        var store = new CacheStore();
        bool r1 = store.TryApplyUpdate("key1", "value1", 1);
        bool r2 = store.TryApplyUpdate("key2", "value1", 1);
        bool r3 = store.TryApplyUpdate("key3", "value1", 1);

        Assert.True(r1 && r2 && r3);
    }
}
