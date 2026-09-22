using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace RavenDBHomeAssignment.Protocol;

/// <summary>
/// TCP frame helpers for length-prefixed message framing.
/// All messages use a 4-byte Int32 little-endian length header followed by UTF-8 JSON payload.
/// </summary>
public static class Framing
{
    private const int HeaderSize = sizeof(int);
    
    /// <summary>
    /// Write a framed message: 4-byte Int32 length header + UTF-8 payload.
    /// </summary>
    public static async Task WriteFramedAsync(Stream stream, byte[] payload, CancellationToken ct)
    {
        if (stream == null) throw new ArgumentNullException(nameof(stream));
        if (payload == null) throw new ArgumentNullException(nameof(payload));

        // Write 4-byte length header (little-endian)
        byte[] header = BitConverter.GetBytes(payload.Length);
        await stream.WriteAsync(header, 0, HeaderSize, ct).ConfigureAwait(false);

        // Write payload
        await stream.WriteAsync(payload, 0, payload.Length, ct).ConfigureAwait(false);
        await stream.FlushAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Read a framed message: reads exact 4-byte Int32 length header, then reads exact payload.
    /// Handles partial socket reads correctly.
    /// Returns the payload bytes (header is stripped).
    /// </summary>
    public static async Task<byte[]> ReadFramedAsync(Stream stream, CancellationToken ct)
    {
        if (stream == null) throw new ArgumentNullException(nameof(stream));

        // Read exact 4-byte header
        byte[] header = new byte[HeaderSize];
        int bytesRead = await ReadExactAsync(stream, header, HeaderSize, ct).ConfigureAwait(false);
        if (bytesRead == 0)
            throw new EndOfStreamException("Stream closed before reading frame header");

        // Parse length
        int payloadLength = BitConverter.ToInt32(header, 0);
        if (payloadLength < 0)
            throw new InvalidOperationException($"Invalid frame length: {payloadLength}");

        // Read exact payload
        byte[] payload = new byte[payloadLength];
        if (payloadLength > 0)
        {
            // Read exact payload bytes
            await ReadExactAsync(stream, payload, payloadLength, ct).ConfigureAwait(false);
        }

        return payload;
    }

    /// <summary>
    /// Helper: Reads exact number of bytes from stream, handling partial reads.
    /// </summary>
    private static async Task<int> ReadExactAsync(Stream stream, byte[] buffer, int count, CancellationToken ct)
    {
        int totalRead = 0;
        while (totalRead < count)
        {
            int bytesRead = await stream.ReadAsync(buffer, totalRead, count - totalRead, ct).ConfigureAwait(false);
            if (bytesRead == 0)
            {
                // Stream closed before reading expected bytes
                return totalRead; // End of stream
            }
            totalRead += bytesRead;
        }
        return totalRead;
    }
}
