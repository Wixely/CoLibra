namespace CoLibra;

/// <summary>
/// Stable identity of a node in the cluster. Generated as a GUID v7 per process by default,
/// or fixed via <see cref="CoLibraOptions.NodeId"/>.
/// </summary>
public readonly record struct NodeId(Guid Value) : IComparable<NodeId>
{
    /// <summary>Creates a new random (GUID v7) node id.</summary>
    public static NodeId NewId() => new(Guid.CreateVersion7());

    /// <inheritdoc />
    public int CompareTo(NodeId other) => Value.CompareTo(other.Value);

    /// <summary>True when <paramref name="left"/> sorts higher than <paramref name="right"/> (bully election precedence).</summary>
    public static bool operator >(NodeId left, NodeId right) => left.CompareTo(right) > 0;

    /// <summary>True when <paramref name="left"/> sorts lower than <paramref name="right"/>.</summary>
    public static bool operator <(NodeId left, NodeId right) => left.CompareTo(right) < 0;

    /// <inheritdoc />
    public override string ToString() => Value.ToString("N")[..8];
}
