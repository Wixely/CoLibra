namespace CoLibra;

/// <summary>
/// Identifies a unit of exclusively-owned work: a user-defined <paramref name="Type"/>
/// (e.g. "sourceid") and an <paramref name="Id"/> within that type (e.g. "source_12345").
/// Comparison is ordinal and case-sensitive.
/// </summary>
public readonly record struct LeaseKey(string Type, string Id)
{
    /// <inheritdoc />
    public override string ToString() => $"{Type}/{Id}";
}
