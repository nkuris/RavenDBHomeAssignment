using System.Text.Json.Serialization;

namespace RavenDBHomeAssignment.Protocol;

/// <summary>
/// Client request message: SET, GET, or DEL operation.
/// </summary>
public class CacheRequest
{
    /// <summary>
    /// Operation: "SET", "GET", or "DEL"
    /// </summary>
    [JsonPropertyName("op")]
    public string Op { get; set; } = string.Empty;

    /// <summary>
    /// Cache key
    /// </summary>
    [JsonPropertyName("key")]
    public string Key { get; set; } = string.Empty;

    /// <summary>
    /// Value for SET operations; null for GET/DEL
    /// </summary>
    [JsonPropertyName("value")]
    public string? Value { get; set; }

    /// <summary>
    /// Minimum LSN for read-your-writes consistency on GET operations.
    /// Replica will delay response until its LSN >= MinLsn.
    /// </summary>
    [JsonPropertyName("minLsn")]
    public long MinLsn { get; set; }
}

/// <summary>
/// Server response message to a client request.
/// </summary>
public class CacheResponse
{
    /// <summary>
    /// Whether the operation succeeded
    /// </summary>
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    /// <summary>
    /// Value retrieved (GET operations only)
    /// </summary>
    [JsonPropertyName("value")]
    public string? Value { get; set; }

    /// <summary>
    /// Current LSN of the node responding to the request.
    /// Set by Primary on writes; set by Replica on reads.
    /// </summary>
    [JsonPropertyName("lsn")]
    public long Lsn { get; set; }

    /// <summary>
    /// Error message if Success is false
    /// </summary>
    [JsonPropertyName("errorMessage")]
    public string? ErrorMessage { get; set; }
}

/// <summary>
/// Internal replication message sent from Primary to Replica over TCP.
/// </summary>
public class ReplicationMessage
{
    /// <summary>
    /// Operation: "SET" or "DEL"
    /// </summary>
    [JsonPropertyName("op")]
    public string Op { get; set; } = string.Empty;

    /// <summary>
    /// Cache key
    /// </summary>
    [JsonPropertyName("key")]
    public string Key { get; set; } = string.Empty;

    /// <summary>
    /// Value for SET operations; null for DEL
    /// </summary>
    [JsonPropertyName("value")]
    public string? Value { get; set; }

    /// <summary>
    /// LSN assigned by Primary. Replica applies only if this LSN > existing LSN.
    /// </summary>
    [JsonPropertyName("lsn")]
    public long Lsn { get; set; }
}
