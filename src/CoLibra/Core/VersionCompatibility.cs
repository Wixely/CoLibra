namespace CoLibra;

/// <summary>
/// Rule deciding whether a peer's service version may participate in the same cluster
/// as the local node. Applied at discovery (incompatible nodes are not surfaced) and by
/// the coordinator at join.
/// </summary>
public sealed class VersionCompatibility
{
    private enum Mode { Any, Strict, MajorMatch, Minimum }

    private readonly Mode _mode;
    private readonly Version? _minimum;

    private VersionCompatibility(Mode mode, Version? minimum = null)
    {
        _mode = mode;
        _minimum = minimum;
    }

    /// <summary>Any service version is accepted.</summary>
    public static VersionCompatibility Any { get; } = new(Mode.Any);

    /// <summary>Peer version must exactly equal the local version.</summary>
    public static VersionCompatibility Strict { get; } = new(Mode.Strict);

    /// <summary>Peer version must have the same major component as the local version.</summary>
    public static VersionCompatibility MajorMatch { get; } = new(Mode.MajorMatch);

    /// <summary>Peer version must be at least <paramref name="minimum"/> (set it to the last compatible release for rolling deploys).</summary>
    public static VersionCompatibility Minimum(Version minimum) =>
        new(Mode.Minimum, minimum ?? throw new ArgumentNullException(nameof(minimum)));

    /// <summary>Evaluates the rule for a peer version against the local version.</summary>
    public bool IsCompatible(Version local, Version peer) => _mode switch
    {
        Mode.Any => true,
        Mode.Strict => local == peer,
        Mode.MajorMatch => local.Major == peer.Major,
        Mode.Minimum => peer >= _minimum,
        _ => false,
    };

    /// <inheritdoc />
    public override string ToString() => _mode == Mode.Minimum ? $"Minimum({_minimum})" : _mode.ToString();
}
