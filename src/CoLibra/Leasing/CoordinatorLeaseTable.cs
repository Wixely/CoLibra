namespace CoLibra.Leasing;

internal readonly record struct GrantDecision(
    NodeId Requester,
    Guid RequestId,
    LeaseKey Key,
    bool Granted,
    FencingToken Token,
    LeaseDenialReason Reason,
    NodeId? CurrentOwner);

internal sealed class AcquireOutcome
{
    /// <summary>The requester's decision, or null when deferred (Other-preference grace window).</summary>
    public GrantDecision? Immediate { get; init; }

    /// <summary>Pending Other-preference requesters resolved (denied) because this acquire took the key.</summary>
    public List<GrantDecision> Resolved { get; init; } = [];
}

internal sealed class SweepOutcome
{
    /// <summary>Keys freed by expiry, with the nodes to notify.</summary>
    public List<(LeaseKey Key, List<NodeId> Interested)> Freed { get; } = [];

    /// <summary>Other-preference requests granted because their grace window elapsed unclaimed.</summary>
    public List<GrantDecision> MaturedGrants { get; } = [];
}

/// <summary>
/// The coordinator's authoritative lease state: entries with monotonic deadlines, per-type
/// per-node counts for load balancing, interest sets for push-invalidation, and deferred
/// <see cref="ProcessingPreference.Other"/> requests. Purely synchronous; the node's actor
/// loop is the single caller. All time math uses relative TimeProvider timestamps.
/// </summary>
internal sealed class CoordinatorLeaseTable
{
    private sealed class Entry
    {
        public required NodeId Owner;
        public required FencingToken Token;
        public required long DeadlineTs;
    }

    private sealed record PendingOther(NodeId Requester, Guid RequestId, long DeadlineTs);

    private readonly CoLibraOptions _options;
    private readonly CompletionRegistry? _completions;
    private readonly long _ttlTicks;
    private readonly long _graceTicks;
    private readonly Dictionary<LeaseKey, Entry> _leases = [];
    private readonly Dictionary<LeaseKey, HashSet<NodeId>> _interest = [];
    private readonly Dictionary<LeaseKey, List<PendingOther>> _pendingOther = [];
    private readonly Dictionary<string, Dictionary<NodeId, int>> _countsByType = new(StringComparer.Ordinal);
    private readonly Dictionary<NodeId, double> _nodeWeights = [];
    private readonly HashSet<NodeId> _notAccepting = [];
    private readonly Dictionary<LeaseKey, (NodeId OldOwner, long UntilTs)> _revoked = [];
    private readonly long _revokeHoldTicks;
    private long _sequence;

    public CoordinatorLeaseTable(long term, CoLibraOptions options, TimeProvider timeProvider,
        CompletionRegistry? completions = null)
    {
        Term = term;
        _options = options;
        _completions = completions;
        _ttlTicks = (long)(options.LeaseTtl.TotalSeconds * timeProvider.TimestampFrequency);
        _graceTicks = (long)(options.OtherPreferenceGraceWindow.TotalSeconds * timeProvider.TimestampFrequency);
        // Hold-down after a forced revocation: long enough for the old owner to hear (push or
        // next renewal) and stop before anyone new can be granted the key.
        _revokeHoldTicks = (long)(options.HeartbeatInterval.TotalSeconds * 2.5 * timeProvider.TimestampFrequency);
    }

    public long Term { get; private set; }

    /// <summary>
    /// Raises the term in place (Forced-coordinator takeover defense). Sequence numbering
    /// continues, so fencing tokens stay strictly monotonic — the term only ever increases.
    /// </summary>
    public void EscalateTerm(long newTerm)
    {
        if (newTerm > Term)
            Term = newTerm;
    }

    public int LeaseCount => _leases.Count;

    /// <summary>Number of member nodes currently draining (not accepting new leases) — diagnostics.</summary>
    public int NotAcceptingCount => _notAccepting.Count;

    public void NodeUp(NodeId node, double weight, bool acceptsWork = true)
    {
        _nodeWeights[node] = weight;
        SetAcceptsWork(node, acceptsWork);
    }

    public void SetWeight(NodeId node, double weight)
    {
        if (_nodeWeights.ContainsKey(node))
            _nodeWeights[node] = weight;
    }

    /// <summary>Marks a node as (not) accepting work: excluded from grants and balance math.</summary>
    public void SetAcceptsWork(NodeId node, bool acceptsWork)
    {
        if (acceptsWork)
            _notAccepting.Remove(node);
        else
            _notAccepting.Add(node);
    }

    /// <summary>Removes a dead node; returns its freed keys with the interest sets to notify.</summary>
    public List<(LeaseKey Key, List<NodeId> Interested)> NodeDown(NodeId node)
    {
        _nodeWeights.Remove(node);
        _notAccepting.Remove(node);
        foreach (var counts in _countsByType.Values)
            counts.Remove(node);

        var freed = new List<(LeaseKey, List<NodeId>)>();
        foreach (var (key, entry) in _leases.Where(kv => kv.Value.Owner == node).ToList())
        {
            _leases.Remove(key);
            freed.Add((key, TakeInterest(key, exclude: node)));
        }

        // The dead node's deferred requests will never be answered; drop them.
        foreach (var (key, pending) in _pendingOther.ToList())
        {
            pending.RemoveAll(p => p.Requester == node);
            if (pending.Count == 0)
                _pendingOther.Remove(key);
        }

        return freed;
    }

    public AcquireOutcome Acquire(NodeId requester, Guid requestId, LeaseKey key, ProcessingPreference preference, long nowTs)
    {
        if (_completions?.Contains(key) == true)
        {
            return new AcquireOutcome
            {
                Immediate = new GrantDecision(requester, requestId, key, false, default, LeaseDenialReason.Completed, null),
            };
        }

        if (_notAccepting.Contains(requester))
        {
            return new AcquireOutcome
            {
                Immediate = new GrantDecision(requester, requestId, key, false, default, LeaseDenialReason.NotAcceptingWork, null),
            };
        }

        // Freshly revoked keys are held down until the old owner has provably stopped;
        // report them as still held so the exclusivity guarantee survives the move.
        if (_revoked.TryGetValue(key, out var revoked))
        {
            if (nowTs < revoked.UntilTs)
            {
                RegisterInterest(key, requester);
                return new AcquireOutcome
                {
                    Immediate = new GrantDecision(requester, requestId, key, false, default, LeaseDenialReason.HeldByOther, revoked.OldOwner),
                };
            }

            _revoked.Remove(key); // expired; fall through to a normal grant
        }

        if (_leases.TryGetValue(key, out var entry))
        {
            if (entry.Owner == requester)
            {
                entry.DeadlineTs = nowTs + _ttlTicks; // idempotent re-acquire refreshes
                return new AcquireOutcome
                {
                    Immediate = new GrantDecision(requester, requestId, key, true, entry.Token, LeaseDenialReason.None, null),
                };
            }

            RegisterInterest(key, requester);
            return new AcquireOutcome
            {
                Immediate = new GrantDecision(requester, requestId, key, false, default, LeaseDenialReason.HeldByOther, entry.Owner),
            };
        }

        if (preference == ProcessingPreference.Other && _nodeWeights.Count > 1)
        {
            // Hold the request open so a willing node can take the key; grant at window end if none does.
            var pending = _pendingOther.TryGetValue(key, out var list) ? list : _pendingOther[key] = [];
            pending.Add(new PendingOther(requester, requestId, nowTs + _graceTicks));
            return new AcquireOutcome();
        }

        if (preference == ProcessingPreference.Balanced && IsOverloaded(requester, key.Type))
        {
            RegisterInterest(key, requester);
            return new AcquireOutcome
            {
                Immediate = new GrantDecision(requester, requestId, key, false, default, LeaseDenialReason.Rebalance, null),
            };
        }

        var outcome = new AcquireOutcome
        {
            Immediate = Grant(requester, requestId, key, nowTs),
        };

        // A willing owner arrived: resolve any parked Other-preference requests as denied.
        if (_pendingOther.Remove(key, out var parked))
        {
            foreach (var other in parked.Where(p => p.Requester != requester))
            {
                RegisterInterest(key, other.Requester);
                outcome.Resolved.Add(new GrantDecision(
                    other.Requester, other.RequestId, key, false, default, LeaseDenialReason.PreferredElsewhere, requester));
            }
        }

        return outcome;
    }

    /// <summary>Releases a key when the caller owns it; returns the nodes to notify.</summary>
    public bool Release(NodeId requester, LeaseKey key, out List<NodeId> interested)
    {
        interested = [];
        if (!_leases.TryGetValue(key, out var entry) || entry.Owner != requester)
            return false;

        _leases.Remove(key);
        AdjustCount(key.Type, requester, -1);
        interested = TakeInterest(key, exclude: requester);
        return true;
    }

    /// <summary>Renews the owner's held leases; returns keys the owner has lost.</summary>
    public List<LeaseKey> Renew(NodeId owner, IReadOnlyList<(LeaseKey Key, FencingToken Token)> held, long nowTs)
    {
        var lost = new List<LeaseKey>();
        foreach (var (key, token) in held)
        {
            if (_leases.TryGetValue(key, out var entry))
            {
                if (entry.Owner == owner)
                    entry.DeadlineTs = nowTs + _ttlTicks;
                else
                    lost.Add(key);
            }
            else if (_completions?.Contains(key) == true)
            {
                lost.Add(key); // completed keys are never re-adopted; the renewer must drop it
            }
            else if (_revoked.ContainsKey(key))
            {
                lost.Add(key); // forcibly rebalanced: the old owner must drop it, not re-adopt it
            }
            else
            {
                // Expired here but the owner still renews (e.g. right after failover): the key is
                // unowned, so re-adopting it is safe and avoids needless churn.
                _leases[key] = new Entry { Owner = owner, Token = token, DeadlineTs = nowTs + _ttlTicks };
                AdjustCount(key.Type, owner, +1);
            }
        }

        return lost;
    }

    /// <summary>
    /// Applies a joining member's held-lease assertions (coordinator failover / partition heal).
    /// Conflicts resolve by fencing token: the lower token loses. Returns the rejected keys.
    /// </summary>
    public List<LeaseKey> AssertHeld(NodeId owner, IReadOnlyList<(LeaseKey Key, FencingToken Token)> asserts, long nowTs)
    {
        var rejected = new List<LeaseKey>();
        foreach (var (key, token) in asserts)
        {
            if (_completions?.Contains(key) == true)
            {
                rejected.Add(key); // the key finished elsewhere while this node was away
                continue;
            }

            if (_revoked.ContainsKey(key))
            {
                rejected.Add(key); // rebalanced away while this node was rejoining
                continue;
            }

            if (_leases.TryGetValue(key, out var entry) && entry.Owner != owner)
            {
                if (entry.Token >= token)
                {
                    rejected.Add(key);
                    continue;
                }

                // The asserter holds the newer grant; the previous owner learns via its next renewal.
                AdjustCount(key.Type, entry.Owner, -1);
            }

            _leases[key] = new Entry { Owner = owner, Token = token, DeadlineTs = nowTs + _ttlTicks };
            AdjustCount(key.Type, owner, +1);
        }

        return rejected;
    }

    public bool TryGetOwner(LeaseKey key, out NodeId owner, out FencingToken token)
    {
        if (_leases.TryGetValue(key, out var entry))
        {
            owner = entry.Owner;
            token = entry.Token;
            return true;
        }

        owner = default;
        token = default;
        return false;
    }

    /// <summary>Issues the token a forced assignment will commit with (two-step: assign message first, commit on ack).</summary>
    public FencingToken NextToken() => new(Term, ++_sequence);

    /// <summary>Force-installs an owner for an unowned key (routed-delivery assignment).</summary>
    public void Assign(NodeId owner, LeaseKey key, FencingToken token, long nowTs)
    {
        _leases[key] = new Entry { Owner = owner, Token = token, DeadlineTs = nowTs + _ttlTicks };
        AdjustCount(key.Type, owner, +1);
        _pendingOther.Remove(key);
    }

    /// <summary>The candidate with the lowest weighted lease count for the type; null when there are no candidates.</summary>
    public NodeId? PickLeastLoaded(string type, IReadOnlyCollection<NodeId> candidates)
    {
        if (candidates.Count == 0)
            return null;

        var counts = _countsByType.GetValueOrDefault(type);
        return candidates
            .OrderBy(n => (counts?.GetValueOrDefault(n) ?? 0) / Math.Max(_nodeWeights.GetValueOrDefault(n, 1.0), 0.001))
            .First();
    }

    /// <summary>Registers a node's interest in a key without acquiring (used for decision-cache invalidation).</summary>
    public void RegisterInterest(LeaseKey key, NodeId node)
    {
        if (!_interest.TryGetValue(key, out var set))
            _interest[key] = set = [];
        set.Add(node);
    }

    /// <summary>Expires overdue leases and matures parked Other-preference requests.</summary>
    public SweepOutcome Sweep(long nowTs)
    {
        var outcome = new SweepOutcome();

        foreach (var (key, entry) in _leases.Where(kv => kv.Value.DeadlineTs <= nowTs).ToList())
        {
            _leases.Remove(key);
            AdjustCount(key.Type, entry.Owner, -1);
            outcome.Freed.Add((key, TakeInterest(key, exclude: entry.Owner)));
        }

        // Revocation hold-downs that have elapsed: the old owner has provably stopped
        // (renewal cycle passed); the keys are now genuinely free — tell the interested.
        foreach (var (key, revoked) in _revoked.Where(kv => kv.Value.UntilTs <= nowTs).ToList())
        {
            _revoked.Remove(key);
            outcome.Freed.Add((key, TakeInterest(key, exclude: revoked.OldOwner)));
        }

        foreach (var (key, pending) in _pendingOther.ToList())
        {
            if (_leases.ContainsKey(key))
            {
                _pendingOther.Remove(key);
                continue;
            }

            if (_completions?.Contains(key) == true)
            {
                // Completed while parked: resolve every waiter as denied instead of granting.
                _pendingOther.Remove(key);
                foreach (var parked in pending)
                {
                    outcome.MaturedGrants.Add(new GrantDecision(
                        parked.Requester, parked.RequestId, key, false, default, LeaseDenialReason.Completed, null));
                }

                continue;
            }

            var matured = pending.FirstOrDefault(p => p.DeadlineTs <= nowTs);
            if (matured is null)
                continue;

            _pendingOther.Remove(key);
            outcome.MaturedGrants.Add(Grant(matured.Requester, matured.RequestId, key, nowTs));
            foreach (var other in pending.Where(p => p != matured))
            {
                RegisterInterest(key, other.Requester);
                outcome.MaturedGrants.Add(new GrantDecision(
                    other.Requester, other.RequestId, key, false, default, LeaseDenialReason.HeldByOther, matured.Requester));
            }
        }

        return outcome;
    }

    public IReadOnlyDictionary<string, Dictionary<NodeId, int>> CountsByType => _countsByType;

    /// <summary>
    /// Revokes the minimum set of leases needed to bring every node within tolerance of the
    /// mean load, per type. Nodes at or below the mean are untouched; overloaded nodes shed
    /// only their excess (newest grants first); non-accepting nodes shed everything (drain).
    /// Revoked keys enter the hold-down (see <see cref="Sweep"/>) and redistribute via normal
    /// steering. Types with <see cref="LoadBalanceType.None"/> are skipped — FCFS by design.
    /// </summary>
    public List<(NodeId Owner, LeaseKey Key)> ForceRebalance(string? typeFilter, long nowTs)
    {
        var revocations = new List<(NodeId, LeaseKey)>();
        List<string> types = typeFilter is not null ? [typeFilter] : [.. _countsByType.Keys];
        foreach (var type in types)
        {
            var balance = _options.PerTypeLoadBalance.TryGetValue(type, out var perType)
                ? perType
                : _options.DefaultLoadBalance;
            if (balance == LoadBalanceType.None)
                continue;
            if (!_countsByType.TryGetValue(type, out var counts) || counts.Count == 0)
                continue;

            double WeightOf(NodeId node) => balance == LoadBalanceType.Weighted
                ? Math.Max(_nodeWeights.GetValueOrDefault(node, 1.0), 0.001)
                : 1.0;

            var accepting = _nodeWeights.Keys.Where(n => !_notAccepting.Contains(n)).ToList();
            if (accepting.Count == 0)
                continue; // nowhere for shed leases to go

            var totalCount = counts.Values.Sum();
            var meanLoad = totalCount / accepting.Sum(WeightOf);

            foreach (var (owner, count) in counts.ToList())
            {
                int targetKeys;
                if (_notAccepting.Contains(owner))
                {
                    targetKeys = 0; // draining: everything moves
                }
                else
                {
                    var load = count / WeightOf(owner);
                    if (load <= meanLoad + _options.LoadBalanceTolerance)
                        continue; // within tolerance: untouched
                    targetKeys = (int)Math.Ceiling(meanLoad * WeightOf(owner));
                }

                var shed = count - targetKeys;
                if (shed <= 0)
                    continue;

                // Newest grants first: long-running work is the last thing to ever move.
                var victims = _leases
                    .Where(kv => kv.Key.Type == type && kv.Value.Owner == owner)
                    .OrderByDescending(kv => kv.Value.Token.Sequence)
                    .Take(shed)
                    .Select(kv => kv.Key)
                    .ToList();
                foreach (var key in victims)
                {
                    _leases.Remove(key);
                    AdjustCount(type, owner, -1);
                    _pendingOther.Remove(key);
                    _revoked[key] = (owner, nowTs + _revokeHoldTicks);
                    revocations.Add((owner, key));
                }
            }
        }

        return revocations;
    }

    private GrantDecision Grant(NodeId requester, Guid requestId, LeaseKey key, long nowTs)
    {
        var token = new FencingToken(Term, ++_sequence);
        _leases[key] = new Entry { Owner = requester, Token = token, DeadlineTs = nowTs + _ttlTicks };
        AdjustCount(key.Type, requester, +1);
        return new GrantDecision(requester, requestId, key, true, token, LeaseDenialReason.None, null);
    }

    private bool IsOverloaded(NodeId requester, string type)
    {
        var balance = _options.PerTypeLoadBalance.TryGetValue(type, out var perType)
            ? perType
            : _options.DefaultLoadBalance;
        if (balance == LoadBalanceType.None || _nodeWeights.Count <= 1)
            return false;

        var counts = _countsByType.TryGetValue(type, out var c) ? c : null;
        double Load(NodeId node)
        {
            var count = counts?.GetValueOrDefault(node) ?? 0;
            if (balance != LoadBalanceType.Weighted)
                return count;
            var weight = _nodeWeights.GetValueOrDefault(node, 1.0);
            return count / Math.Max(weight, 0.001);
        }

        // Only nodes accepting work are steering candidates: an authority node that never
        // takes leases must not pin the minimum at zero and starve every real worker.
        var candidates = _nodeWeights.Keys.Where(n => !_notAccepting.Contains(n)).ToList();
        if (candidates.Count <= 1)
            return false;

        var minLoad = candidates.Min(Load);
        return Load(requester) > minLoad + _options.LoadBalanceTolerance;
    }

    private void AdjustCount(string type, NodeId node, int delta)
    {
        if (!_countsByType.TryGetValue(type, out var counts))
            _countsByType[type] = counts = [];
        var next = counts.GetValueOrDefault(node) + delta;
        if (next <= 0)
            counts.Remove(node);
        else
            counts[node] = next;
    }

    private List<NodeId> TakeInterest(LeaseKey key, NodeId exclude)
    {
        if (!_interest.Remove(key, out var set))
            return [];
        set.Remove(exclude);
        return [.. set];
    }
}
