namespace CoLibra;

/// <summary>
/// Monotonic token identifying a specific grant of a lease. Tokens issued by later
/// coordinators (higher <see cref="Term"/>) or later grants (higher <see cref="Sequence"/>)
/// compare greater. Use it to fence writes against external systems: a writer holding a
/// lower token than the one last seen must be a stale owner.
/// </summary>
public readonly record struct FencingToken(long Term, long Sequence) : IComparable<FencingToken>
{
    /// <inheritdoc />
    public int CompareTo(FencingToken other)
    {
        var byTerm = Term.CompareTo(other.Term);
        return byTerm != 0 ? byTerm : Sequence.CompareTo(other.Sequence);
    }

    /// <summary>True when <paramref name="left"/> supersedes <paramref name="right"/>.</summary>
    public static bool operator >(FencingToken left, FencingToken right) => left.CompareTo(right) > 0;

    /// <summary>True when <paramref name="left"/> is superseded by <paramref name="right"/>.</summary>
    public static bool operator <(FencingToken left, FencingToken right) => left.CompareTo(right) < 0;

    /// <summary>True when <paramref name="left"/> compares at or above <paramref name="right"/>.</summary>
    public static bool operator >=(FencingToken left, FencingToken right) => left.CompareTo(right) >= 0;

    /// <summary>True when <paramref name="left"/> compares at or below <paramref name="right"/>.</summary>
    public static bool operator <=(FencingToken left, FencingToken right) => left.CompareTo(right) <= 0;

    /// <inheritdoc />
    public override string ToString() => $"{Term}.{Sequence}";
}
