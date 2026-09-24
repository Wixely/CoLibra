using System.Text.Json.Serialization;

namespace CoLibra.Sample.Router;

// Source-generated JSON for this sample's payloads. The published executables are trimmed with
// reflection-based JSON switched off, so every type that travels between nodes has to be listed
// here — which also means no reflection metadata is built at runtime.
[JsonSerializable(typeof(OrderUpdate))]
internal sealed partial class OrderJsonContext : JsonSerializerContext;
