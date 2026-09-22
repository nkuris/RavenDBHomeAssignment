using System;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using RavenDBHomeAssignment.Protocol;

namespace RavenDBHomeAssignment.Client;

/// <summary>
/// Test client for cache operations.
/// Wraps a TcpClient connection and tracks the last observed LSN for read-your-writes consistency.
/// </summary>
public class CacheClient : IAsyncDisposable
{
    private readonly TcpClient _client;
    private NetworkStream? _stream;
    private long _lastObservedLsn = 0;

    /// <summary>
    /// Connect to a cache node.
    /// </summary>
    public static async Task<CacheClient> ConnectAsync(string host, int port, CancellationToken ct = default)
    {
        var client = new TcpClient();
        await client.ConnectAsync(host, port, ct).ConfigureAwait(false);
        return new CacheClient(client);
    }

    /// <summary>
    /// Internal constructor for connected client.
    /// </summary>
    private CacheClient(TcpClient client)
    {
        _client = client;
        _stream = client.GetStream();
    }

    /// <summary>
    /// Write a value to the cache (SET operation).
    /// Automatically updates the tracked LSN for read-your-writes consistency.
    /// </summary>
    public async Task<bool> SetAsync(string key, string value, CancellationToken ct = default)
    {
        // Validate input
        ArgumentNullException.ThrowIfNull(key);
        // Validate input
        ArgumentNullException.ThrowIfNull(value);

        // Create request
        var request = new CacheRequest
        {
            Op = "SET",
            Key = key,
            Value = value,
            MinLsn = 0
        };

        // Send request and get response
        var response = await SendRequestAsync(request, ct).ConfigureAwait(false);
        if (response.Success)
        {
            // Update last observed LSN for read-your-writes consistency
            _lastObservedLsn = response.Lsn;
        }
        return response.Success;
    }

    /// <summary>
    /// Read a value from the cache (GET operation).
    /// Automatically uses the last observed LSN from writes to ensure read-your-writes consistency.
    /// </summary>
    public async Task<string?> GetAsync(string key, CancellationToken ct = default)
    {
        // Validate input
        ArgumentNullException.ThrowIfNull(key);

        // Create request with MinLsn set to last observed LSN for read-your-writes consistency
        var request = new CacheRequest
        {
            Op = "GET",
            Key = key,
            Value = null,
            MinLsn = _lastObservedLsn  // Automatic read-your-writes: wait for replica to catch up
        };

        // Send request and get response
        var response = await SendRequestAsync(request, ct).ConfigureAwait(false);
        if (response.Success)
            _lastObservedLsn = Math.Max(_lastObservedLsn, response.Lsn);
        return response.Success ? response.Value : null;
    }

    /// <summary>
    /// Delete a key from the cache (DEL operation).
    /// Automatically updates the tracked LSN.
    /// </summary>
    public async Task<bool> DeleteAsync(string key, CancellationToken ct = default)
    {
        // Validate input
        ArgumentNullException.ThrowIfNull(key);

        // Create request
        var request = new CacheRequest
        {
            Op = "DEL",
            Key = key,
            Value = null,
            MinLsn = 0
        };
        
        // Send request and get response
        var response = await SendRequestAsync(request, ct).ConfigureAwait(false);
        // Update last observed LSN if successful
        if (response.Success)
        {
            _lastObservedLsn = response.Lsn;
        }
        return response.Success;
    }

    /// <summary>
    /// Get the last observed LSN from the server (for testing).
    /// </summary>
    public long LastObservedLsn => _lastObservedLsn;

    /// <summary>
    /// Send a request and receive a response.
    /// Internal helper for all operations.
    /// </summary>
    private async Task<CacheResponse> SendRequestAsync(CacheRequest request, CancellationToken ct)
    {
        if (_stream == null)
            throw new ObjectDisposedException("CacheClient: stream closed");

        // Serialize request to JSON
        byte[] requestBytes = JsonSerializer.SerializeToUtf8Bytes(request);

        // Send framed request  
        await Framing.WriteFramedAsync(_stream, requestBytes, ct).ConfigureAwait(false);

        // Read framed response
        byte[] responseBytes = await Framing.ReadFramedAsync(_stream, ct).ConfigureAwait(false);
        var response = JsonSerializer.Deserialize<CacheResponse>(responseBytes);

        return response ?? new() { Success = false, ErrorMessage = "Deserialization failed" };
    }

    /// <summary>
    /// Graceful shutdown.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_stream != null)
        {
            await _stream.FlushAsync().ConfigureAwait(false);
            _stream.Dispose();
        }
        _client?.Dispose();
    }
}
