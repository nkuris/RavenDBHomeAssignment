using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace RavenDBHomeAssignment.Server;

/// <summary>
/// In-memory cache store with idempotent updates and read-your-writes consistency.
/// Stores entries as (string Value, long Lsn) tuples to enable idempotency checks.
/// </summary>
public class CacheStore
{
    /// <summary>
    /// Concurrent dictionary storing key -> (Value, LSN) tuples.
    /// </summary>
    private readonly ConcurrentDictionary<string, (string Value, long Lsn)> _store = new();

    /// <summary>
    /// Current LSN of the store (the highest LSN applied so far).
    /// </summary>
    private long _currentLsn = 0;

    /// <summary>
    /// Lock object for synchronizing access to _currentLsn.
    /// </summary>
    private readonly object _lsnLock = new();

    /// <summary>
    /// Attempt to apply an update idempotently.
    /// Updates the key only if the new LSN is strictly greater than the existing LSN (or key doesn't exist).
    /// Returns true if applied, false if rejected (older or equal LSN).
    /// </summary>
    public bool TryApplyUpdate(string key, string value, long lsn)
    {
        // Validate inputs
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        // Reject non-positive LSNs (LSN must be > 0)
        if (lsn <= 0)
        {
            // Invalid LSN, reject the update
            return false;
        }

        bool applied = false;
        // Factory function to create a new entry if the key does not exist
        // This will be called only if the key is not present in the dictionary
        Func<string, (string Value, long Lsn)> addValueFactory = _ =>
            {
                applied = true;
                return (value, lsn);
            };
        // Use AddOrUpdate to atomically insert or update the key
        _store.AddOrUpdate(
            key,
            // If key doesn't exist, always insert
            addValueFactory,
            // If key exists, only update if new LSN is greater
            (elementKey, existing) =>
            {
                // Only update if new LSN is greater than existing LSN
                if (lsn > existing.Lsn)
                {
                    // Update the value and LSN
                    applied = true;
                    return (value, lsn);
                }
                return existing;
            }
        );

        return applied;
    }

    /// <summary>
    /// Retrieve a value with read-your-writes consistency.
    /// If minLsn > 0, waits asynchronously until the store's CurrentLsn >= minLsn before returning.
    /// This ensures the Replica has caught up to at least the client's last observed write.
    /// Returns null if the key does not exist after LSN requirement is met.
    /// </summary>
    public async Task<string?> GetAsync(string key, long minLsn, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(key);

        // Wait until CurrentLsn >= minLsn (blocking for read-your-writes consistency)
        while (true)
        {
            lock (_lsnLock)
            {
                if (_currentLsn >= minLsn)
                    break; // LSN requirement met, proceed with read
            }

            ct.ThrowIfCancellationRequested();
            // Poll asynchronously with 10ms delay until LSN advances
            await Task.Delay(10, ct).ConfigureAwait(false);
        }

        // Retrieve and return the value (or null if key not found)
        _store.TryGetValue(key, out var entry);
        return entry.Value;
    }

    /// <summary>
    /// Update the store's current LSN to the maximum of the current and new value.
    /// Called by Primary after assigning LSN, or by Replica after applying a replication message.
    /// </summary>
    public void UpdateLsn(long lsn)
    {
        lock (_lsnLock)
        {
            if (lsn > _currentLsn)
            {
                _currentLsn = lsn;
            }
        }
    }

    /// <summary>
    /// Get the current LSN of this store (the highest LSN applied so far).
    /// </summary>
    public long CurrentLsn
    {
        get
        {
            lock (_lsnLock)
            {
                return _currentLsn;
            }
        }
    }

    /// <summary>
    /// Delete a key from the store (for testing / admin use).
    /// </summary>
    public bool TryDelete(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return _store.TryRemove(key, out _);
    }

    /// <summary>
    /// Get the total number of entries in the store.
    /// </summary>
    public int Count => _store.Count;

    /// <summary>
    /// Clear all entries from the store (for testing).
    /// </summary>
    public void Clear()
    {
        _store.Clear();
    }
}
