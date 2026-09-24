using System.Text.Json.Serialization;

namespace CoLibra.Sample.HostedMaze;

// Source-generated JSON for this sample's payloads. The published executables are trimmed with
// reflection-based JSON switched off, so every type that travels between nodes has to be listed
// here — which also means no reflection metadata is built at runtime.
[JsonSerializable(typeof(Join))]
[JsonSerializable(typeof(Move))]
[JsonSerializable(typeof(Snapshot))]
internal sealed partial class MazeJsonContext : JsonSerializerContext;
