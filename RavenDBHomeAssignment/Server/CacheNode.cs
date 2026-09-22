using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using RavenDBHomeAssignment.Protocol;

namespace RavenDBHomeAssignment.Server;

/// <summary>
/// A cache node that can operate as either Primary (writer) or Replica (reader).
/// Primary nodes accept writes, assign LSNs, replicate to replicas, and serve reads.
/// Replica nodes listen for replication streams from Primary and serve reads with read-your-writes support.
/// </summary>
public class CacheNode : IAsyncDisposable
{
    /// Node configuration
    /// <summary>
    /// True if this node is Primary (writer), false if Replica (reader).
    /// </summary>
    private readonly bool _isPrimary;

    /// <summary>
    /// Port to listen on for client connections.
    /// </summary>
    private readonly int _port;

    /// <summary>
    /// List of (host, port) addresses for replicas to replicate to (Primary only).
    /// </summary>
    private readonly IReadOnlyList<(string Host, int Port)> _replicaAddresses;

    /// <summary>
    /// In-memory cache store for this node, supporting idempotent updates and read-your-writes consistency.
    /// </summary>
    private readonly CacheStore _store;

    /// <summary>
    /// Public accessor for the cache store.
    /// </summary>
    public CacheStore Store => _store;

    /// <summary>
    /// List of replication pipelines to connected replicas (Primary only).
    /// </summary>
    private readonly List<ReplicationPipeline> _replicationPipelines;

    /// <summary>
    /// Number of connected replication pipelines.
    /// </summary>
    public int ConnectedReplicaCount => _replicationPipelines.Count;

    /// <summary>
    /// TCP listener for accepting client connections (both Primary and Replica).
    /// </summary>
    private TcpListener? _listener;

    /// <summary>
    /// TCP listener for accepting replication connections from Primary (Replica only).
    /// Uses port = _port + 1000 to avoid conflicts with client listener.
    /// </summary>
    private TcpListener? _replicationListener;

    private long _globalLsn = 0;

    /// <summary>
    /// Event signaled when ConnectToReplicasAsync completes initial connection attempts.
    /// </summary>
    private readonly TaskCompletionSource _connectionsCompletedTcs = new();

    /// <summary>
    /// Create a new cache node.
    /// </summary>
    /// <param name="isPrimary">True for Primary (writer), false for Replica (reader)</param>
    /// <param name="port">Port to listen on for client connections</param>
    /// <param name="replicaAddresses">List of (host, port) for replicas (Primary only)</param>
    public CacheNode(bool isPrimary, int port, IReadOnlyList<(string Host, int Port)>? replicaAddresses = null)
    {
        _isPrimary = isPrimary;
        _port = port;
        _replicaAddresses = replicaAddresses ?? new List<(string, int)>();
        _store = new CacheStore();
        _replicationPipelines = new List<ReplicationPipeline>();
    }

    /// <summary>
    /// Start the node: establish replication connections (Primary), listen for client connections.
    /// </summary>
    public async Task StartAsync(CancellationToken ct)
    {
        // Start TCP listener for client connections
        _listener = new TcpListener(IPAddress.Loopback, _port);
        _listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        _listener.Start();

        // Start background tasks based on role
        if (_isPrimary)
        {
            // Start accepting client write connections
            _ = AcceptClientWritesAsync(ct);

            // Start connecting to replicas and WAIT for completion before returning
            await ConnectToReplicasAsync(ct);
        }
        else
        {
            // Replica: start replication listener on port + 1000 to accept replication from Primary
            int replicationPort = _port + 1000;
            _replicationListener = new TcpListener(IPAddress.Loopback, replicationPort);
            _replicationListener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            _replicationListener.Start();

            // Start accepting replication connections from Primary
            _ = AcceptReplicationConnectionAsync(ct);

            // Start accepting client read connections on main port
            _ = AcceptClientReadsAsync(ct);
        }
    }

    /// <summary>
    /// Background task to connect to replicas with retry logic.
    /// </summary>
    private async Task ConnectToReplicasAsync(CancellationToken ct)
    {
        foreach (var (host, clientPort) in _replicaAddresses)
        {
            int replicationPort = clientPort + 1000;
            Console.WriteLine($"[DEBUG] Starting connection attempts to {host}:{replicationPort}");

            // Retry with exponential backoff, starting fast and then slowing down
            int maxRetries = 50;  // More retries to give replicas time to start
            int delayMs = 10;     // Start with 10ms delay instead of 100ms

            for (int attempt = 0; attempt < maxRetries; attempt++)
            {
                Console.WriteLine($"[DEBUG]   Attempt {attempt + 1} starting...");
                if (ct.IsCancellationRequested)
                    return;

                try
                {
                    var replicaClient = new TcpClient();
                    // Use WaitAsync with timeout instead of relying on CancellationToken
                    await replicaClient.ConnectAsync(host, replicationPort).WaitAsync(TimeSpan.FromSeconds(1), ct).ConfigureAwait(false);
                    var pipeline = new ReplicationPipeline(replicaClient.GetStream());
                    _replicationPipelines.Add(pipeline);
                    Console.WriteLine($"[DEBUG] ? Connected to {host}:{replicationPort}, now have {_replicationPipelines.Count} pipelines");
                    // Start background replication task
                    _ = pipeline.RunAsync(ct);
                    break;  // Connected successfully
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[DEBUG] ? Attempt {attempt + 1} to {host}:{replicationPort} failed: {ex.GetType().Name}: {ex.Message}");
                    if (attempt < maxRetries - 1)
                    {
                        await Task.Delay(delayMs, ct).ConfigureAwait(false);
                        delayMs = Math.Min(delayMs * 2, 2000);  // Exponential backoff, max 2 seconds
                    }
                }
            }
            Console.WriteLine($"[DEBUG] Finished with {host}:{replicationPort}");
        }
        Console.WriteLine($"[DEBUG] ConnectToReplicasAsync complete. Total pipelines: {_replicationPipelines.Count}");
    }

    /// <summary>
    /// Accept incoming replication connections from Primary (Replica only).
    /// </summary>
    private async Task AcceptReplicationConnectionAsync(CancellationToken ct)
    {
        if (_replicationListener == null)
        {
            return;
        }

        while (!ct.IsCancellationRequested)
        {
            try
            {
                // Accept a replication connection from Primary
                TcpClient client = await _replicationListener.AcceptTcpClientAsync(ct).ConfigureAwait(false);
                // IMPORTANT: Don't dispose the client! Pass it to ListenForReplicationAsync which will use and dispose it
                using (client)
                {
                    await ListenForReplicationAsync(client.GetStream(), ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                if (ex.GetType().Name == "ObjectDisposedException")
                    break;
                // Continue retrying on other errors
            }
        }
    }

    /// <summary>
    /// Accept and handle client write connections (Primary only).
    /// </summary>
    private async Task AcceptClientWritesAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                TcpClient client = await _listener!.AcceptTcpClientAsync(ct).ConfigureAwait(false);
                // Handle each client in the background
                _ = HandlePrimaryClientAsync(client, ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Accept error: {ex.Message}");
            }
        }
    }

    /// <summary>
    /// Handle a client connection on Primary (SET, GET, DEL) - handles multiple requests.
    /// </summary>
    private async Task HandlePrimaryClientAsync(TcpClient client, CancellationToken ct)
    {
        using (client)
        using (var stream = client.GetStream())
        {
            try
            {
                // Handle multiple requests on this connection
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        // Read framed request
                        byte[] requestBytes = await Framing.ReadFramedAsync(stream, ct).ConfigureAwait(false);
                        var request = JsonSerializer.Deserialize<CacheRequest>(requestBytes);
                        if (request == null)
                            break;

                        // Route to handler
                        CacheResponse response = request.Op switch
                        {
                            "SET" => await HandleSetAsync(request, ct),
                            "GET" => await HandleGetPrimary(request, ct),
                            "DEL" => await HandleDeleteAsync(request, ct),
                            _ => new() { Success = false, ErrorMessage = "Unknown operation" }
                        };

                        // Send framed response
                        byte[] responseBytes = JsonSerializer.SerializeToUtf8Bytes(response);
                        await Framing.WriteFramedAsync(stream, responseBytes, ct).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }
            catch (EndOfStreamException)
            {
                // Client disconnected
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Primary client error: {ex.Message}");
            }
        }
    }

    /// <summary>
    /// Handle SET operation: increment LSN, apply locally, replicate to replicas.
    /// </summary>
    private async Task<CacheResponse> HandleSetAsync(CacheRequest request, CancellationToken ct)
    {
        long newLsn = Interlocked.Increment(ref _globalLsn);
        _store.TryApplyUpdate(request.Key, request.Value ?? string.Empty, newLsn);
        _store.UpdateLsn(newLsn);
        Console.WriteLine($"[Primary] SET {request.Key}={request.Value} assigned LSN={newLsn}, enqueueing to {_replicationPipelines.Count} replicas");

        // Enqueue to all replica pipelines (SYNCHRONOUS for visibility)
        foreach (var pipeline in _replicationPipelines)
        {
            try
            {
                await pipeline.EnqueueAsync(
                    new ReplicationMessage
                    {
                        Op = "SET",
                        Key = request.Key,
                        Value = request.Value,
                        Lsn = newLsn
                    },
                    ct
                ).ConfigureAwait(false);
                Console.WriteLine($"[Primary] ? Enqueued SET for {request.Key} to replica");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Primary] ? Failed to enqueue SET: {ex.Message}");
            }
        }

        return new() { Success = true, Lsn = newLsn };
    }

    /// <summary>
    /// Handle GET operation on Primary.
    /// </summary>
    private async Task<CacheResponse> HandleGetPrimary(CacheRequest request, CancellationToken ct)
    {
        string? value = await _store.GetAsync(request.Key, request.MinLsn, ct).ConfigureAwait(false);
        return new() { Success = true, Value = value, Lsn = _store.CurrentLsn };
    }

    /// <summary>
    /// Handle DELETE operation: increment LSN, apply locally, replicate to replicas.
    /// </summary>
    private async Task<CacheResponse> HandleDeleteAsync(CacheRequest request, CancellationToken ct)
    {
        // Increment LSN and apply delete locally
        long newLsn = Interlocked.Increment(ref _globalLsn);
        _store.TryDelete(request.Key);
        _store.UpdateLsn(newLsn);
        Console.WriteLine($"[Primary] DEL {request.Key} assigned LSN={newLsn}, enqueueing to {_replicationPipelines.Count} replicas");

        // Enqueue to all replica pipelines (SYNCHRONOUS)
        foreach (var pipeline in _replicationPipelines)
        {
            try
            {
                await pipeline.EnqueueAsync(
                    new ReplicationMessage
                    {
                        Op = "DEL",
                        Key = request.Key,
                        Lsn = newLsn
                    },
                    ct
                ).ConfigureAwait(false);
                Console.WriteLine($"[Primary] ? Enqueued DEL for {request.Key} to replica");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Primary] ? Failed to enqueue DEL: {ex.Message}");
            }
        }

        return new() { Success = true, Lsn = newLsn };
    }

    /// <summary>
    /// Accept and handle client read connections (Replica only).
    /// </summary>
    private async Task AcceptClientReadsAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                TcpClient client = await _listener!.AcceptTcpClientAsync(ct).ConfigureAwait(false);
                // Handle each client in the background
                _ = HandleReplicaClientAsync(client, ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Accept error: {ex.Message}");
            }
        }
    }

    /// <summary>
    /// Handle a client connection on Replica (GET only) - handles multiple requests.
    /// </summary>
    private async Task HandleReplicaClientAsync(TcpClient client, CancellationToken ct)
    {
        using (client)
        using (var stream = client.GetStream())
        {
            try
            {
                // Handle multiple requests on this connection
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        // Read framed request
                        byte[] requestBytes = await Framing.ReadFramedAsync(stream, ct).ConfigureAwait(false);
                        var request = JsonSerializer.Deserialize<CacheRequest>(requestBytes);
                        if (request == null)
                            break;

                        // Replica only handles GET
                        CacheResponse response;
                        if (request.Op != "GET")
                        {
                            response = new() { Success = false, ErrorMessage = "Replica only supports GET operations" };
                        }
                        else
                        {
                            string? value = await _store.GetAsync(request.Key, request.MinLsn, ct).ConfigureAwait(false);
                            response = new() { Success = true, Value = value, Lsn = _store.CurrentLsn };
                        }

                        // Send framed response
                        byte[] responseBytes = JsonSerializer.SerializeToUtf8Bytes(response);
                        await Framing.WriteFramedAsync(stream, responseBytes, ct).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }
            catch (EndOfStreamException)
            {
                // Client disconnected
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Replica client error: {ex.Message}");
            }
        }
    }

    /// <summary>
    /// Listen for replication messages from Primary (Replica only).
    /// </summary>
    private async Task ListenForReplicationAsync(NetworkStream stream, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                // Read framed replication message from Primary
                byte[] messageBytes = await Framing.ReadFramedAsync(stream, ct).ConfigureAwait(false);
                var message = JsonSerializer.Deserialize<ReplicationMessage>(messageBytes);
                if (message == null)
                    continue;

                // Apply to store idempotently (LSN check)
                if (message.Op == "SET")
                {
                    _store.TryApplyUpdate(message.Key, message.Value ?? string.Empty, message.Lsn);
                }
                else if (message.Op == "DEL")
                {
                    // For idempotent DELETE, only delete if LSN is higher
                    if (message.Lsn > _store.CurrentLsn)
                    {
                        _store.TryDelete(message.Key);
                    }
                }

                // Update cluster LSN to signal any waiting readers
                _store.UpdateLsn(message.Lsn);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown
        }
        catch (Exception ex)
        {
            // Log but don't crash
        }
    }

    /// <summary>
    /// Graceful shutdown.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        _listener?.Stop();
        _listener?.Dispose();
        _replicationListener?.Stop();
        _replicationListener?.Dispose();
        foreach (var pipeline in _replicationPipelines)
        {
            if (pipeline is IAsyncDisposable disposable)
                await disposable.DisposeAsync();
        }
        // Give OS time to release sockets
        await Task.Delay(100);
    }
}
