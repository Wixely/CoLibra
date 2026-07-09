using System.Text.Json;

namespace CoLibra;

/// <summary>
/// Converts routed payload objects to and from the raw bytes that travel between nodes.
/// The default is <see cref="JsonPayloadSerializer"/> (System.Text.Json — zero dependencies);
/// plug in MessagePack, MemoryPack or any other serializer by implementing these two methods
/// and assigning <see cref="RoutingOptions.PayloadSerializer"/>. All nodes must use the same
/// serializer for a given payload type.
/// </summary>
public interface IRoutedPayloadSerializer
{
    /// <summary>Converts a payload object to the bytes that travel on the wire.</summary>
    byte[] Serialize<T>(T value);

    /// <summary>Reconstructs the payload object from received bytes.</summary>
    T? Deserialize<T>(ReadOnlyMemory<byte> payload);
}

/// <summary>
/// The built-in serializer: System.Text.Json (UTF-8). Supply your own
/// <see cref="JsonSerializerOptions"/> (e.g. with a source-generated TypeInfoResolver for
/// Native AOT, or custom converters) via the constructor.
/// </summary>
public sealed class JsonPayloadSerializer(JsonSerializerOptions? options = null) : IRoutedPayloadSerializer
{
    private readonly JsonSerializerOptions _options = options ?? JsonSerializerOptions.Default;

    /// <inheritdoc />
    public byte[] Serialize<T>(T value) => JsonSerializer.SerializeToUtf8Bytes(value, _options);

    /// <inheritdoc />
    public T? Deserialize<T>(ReadOnlyMemory<byte> payload) => JsonSerializer.Deserialize<T>(payload.Span, _options);
}
