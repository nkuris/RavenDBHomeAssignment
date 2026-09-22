using System;
using System.IO;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using RavenDBHomeAssignment.Protocol;

namespace RavenDBHomeAssignment.Server;

/// <summary>
/// Background replication pipeline for a single replica.
/// Manages a bounded channel of replication messages, frames them, and sends them over TCP.
/// Provides backpressure: if the channel is full, the Primary write loop will block when enqueueing.
/// </summary>
public class ReplicationPipeline
{
    private readonly Channel<ReplicationMessage> _channel;
    private readonly NetworkStream _stream;

    /// <summary>
    /// Create a new replication pipeline.
    /// </summary>
    /// <param name="stream">The TCP NetworkStream to the replica.</param>
    public ReplicationPipeline(NetworkStream stream)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));

        // Create bounded channel with backpressure (FullMode.Wait)
        var options = new BoundedChannelOptions(1000)
        {
            FullMode = BoundedChannelFullMode.Wait
        };
        _channel = Channel.CreateBounded<ReplicationMessage>(options);
    }

    /// <summary>
    /// Enqueue a replication message for transmission to the replica.
    /// May block if channel is full (backpressure from slow replica reader).
    /// Call this from the Primary write loop.
    /// </summary>
    public async Task EnqueueAsync(ReplicationMessage message, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(message);
        // Write to the channel (may block if full)
        await _channel.Writer.WriteAsync(message, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Run the background replication task.
    /// Continuously reads messages from the channel, frames them as JSON, and sends over TCP.
    /// Should be started as a background task before the Primary accepts client writes.
    /// Handles graceful shutdown on cancellation.
    /// </summary>
    public async Task RunAsync(CancellationToken ct)
    {
        try
        {
            // Read all messages from the channel (blocked until cancelled or writer completes)
            await foreach (ReplicationMessage message in _channel.Reader.ReadAllAsync(ct))
            {
                // Serialize message to JSON bytes
                byte[] payload = JsonSerializer.SerializeToUtf8Bytes(message);

                // Frame and send over TCP to replica
                await Framing.WriteFramedAsync(_stream, payload, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown
        }
        catch (Exception ex)
        {
            // Log unexpected errors (in production, would log to structured logger)
            System.Diagnostics.Debug.WriteLine($"ReplicationPipeline error: {ex.Message}");
        }
        finally
        {
            // Signal that no more messages will be written
            _channel.Writer.TryComplete();
        }
    }

    /// <summary>
    /// Get the reader end of the channel (for testing or monitoring queue state).
    /// </summary>
    public ChannelReader<ReplicationMessage> Reader => _channel.Reader;
}
