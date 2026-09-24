using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

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
/// <remarks>
/// Trimmed and Native AOT apps should pass options whose <see cref="JsonSerializerOptions.TypeInfoResolver"/>
/// is a source-generated <see cref="System.Text.Json.Serialization.JsonSerializerContext"/>: the
/// reflection-based fallback can lose members the trimmer could not see being used. With no resolver
/// this type uses reflection, and throws if the app disabled it (JsonSerializerIsReflectionEnabledByDefault=false).
/// </remarks>
public sealed class JsonPayloadSerializer : IRoutedPayloadSerializer
{
    private readonly JsonSerializerOptions? _configured;
    private JsonSerializerOptions? _effective;

    /// <summary>Creates the serializer, optionally with custom <see cref="JsonSerializerOptions"/>.</summary>
    public JsonPayloadSerializer(JsonSerializerOptions? options = null) => _configured = options;

    /// <inheritdoc />
    public byte[] Serialize<T>(T value) => JsonSerializer.SerializeToUtf8Bytes(value, TypeInfo<T>());

    /// <inheritdoc />
    public T? Deserialize<T>(ReadOnlyMemory<byte> payload) => JsonSerializer.Deserialize(payload.Span, TypeInfo<T>());

    // Resolving JsonTypeInfo<T> off the options keeps the hot path off the JsonSerializer overloads
    // that take JsonSerializerOptions — those are the trim-unsafe ones.
    private JsonTypeInfo<T> TypeInfo<T>() => (JsonTypeInfo<T>)(_effective ??= Resolve(_configured)).GetTypeInfo(typeof(T));

    // Deliberately deferred to first use rather than done in the constructor: options objects are
    // built (and this serializer defaulted into them) long before anyone sends a payload, and an app
    // that has disabled reflection-based JSON usually replaces the serializer before it matters.
    private static JsonSerializerOptions Resolve(JsonSerializerOptions? options)
    {
        if (options?.TypeInfoResolver is not null)
        {
            options.MakeReadOnly();
            return options;
        }

        // No resolver supplied: fall back to reflection. The switch is what lets a trimmed app be
        // genuinely safe here — build with JsonSerializerIsReflectionEnabledByDefault=false and the
        // trimmer drops this branch, and the reflection-based serializer with it.
        // The analyzer cannot see that the branch is switch-guarded, so it is silenced here only:
        // ILLink is unaffected by the pragma and still reports the reflection dependency to any app
        // that trims with reflection-based JSON left on.
#pragma warning disable IL2026 // reachable only when reflection-based JSON is enabled
        if (!JsonSerializer.IsReflectionEnabledByDefault)
        {
            throw new InvalidOperationException(
                "JsonPayloadSerializer fell back to reflection-based JSON, which this app disabled " +
                "(JsonSerializerIsReflectionEnabledByDefault=false). Construct it with JsonSerializerOptions " +
                "whose TypeInfoResolver is a source-generated JsonSerializerContext, or set " +
                $"{nameof(RoutingOptions.PayloadSerializer)}/{nameof(MessagingOptions.PayloadSerializer)} " +
                $"to your own {nameof(IRoutedPayloadSerializer)}.");
        }

        if (options is null)
        {
            return JsonSerializerOptions.Default;
        }

        // Same thing JsonSerializer.Serialize(value, options) would do on first use.
        options.MakeReadOnly(populateMissingResolver: true);
        return options;
#pragma warning restore IL2026
    }
}
