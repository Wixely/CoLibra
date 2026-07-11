using System.Net;
using CoLibra.Runtime;
using CoLibra.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit.Sdk;

namespace CoLibra.IntegrationTests;

/// <summary>
/// Spins up in-process nodes over the in-memory transport fabric with short (real-time)
/// intervals, and exposes scripted partitions.
/// </summary>
internal sealed class TestCluster(ITestOutputHelper? output = null) : IAsyncDisposable
{
    private readonly InMemoryHub _hub = new();
    private readonly Dictionary<CoLibraNode, IPEndPoint> _endpoints = [];
    private readonly List<CoLibraNode> _nodes = [];

    // CI runners (2 cores) stall the real-time cluster timers long enough to trip the aggressive
    // local timings, killing perfectly healthy nodes. A generous 5x under CI gives timeouts
    // enough headroom to absorb thread-pool starvation and eliminate the rare load flake.
    public static readonly int Scale = Environment.GetEnvironmentVariable("CI") is null ? 1 : 5;

    public static readonly TimeSpan Eventually = TimeSpan.FromSeconds(15 * Scale);

    public Task<CoLibraNode> StartNodeAsync(Action<CoLibraOptions>? mutate = null, bool waitForCluster = true) =>
        StartNodeAsync(mutate, waitForCluster, udpEngine: null);

    public async Task<CoLibraNode> StartNodeAsync(Action<CoLibraOptions>? mutate, bool waitForCluster, IUdpMessagingEngine? udpEngine)
    {
        var options = new CoLibraOptions
        {
            ServiceId = "svc",
            SharedSecret = "test-secret",
            AnnounceInterval = TimeSpan.FromMilliseconds(150 * Scale),
            HeartbeatInterval = TimeSpan.FromMilliseconds(100 * Scale),
            MemberTimeout = TimeSpan.FromMilliseconds(600 * Scale),
            ElectionTimeout = TimeSpan.FromMilliseconds(400 * Scale),
            RebuildWindow = TimeSpan.FromMilliseconds(200 * Scale),
            DiscoveryWindow = TimeSpan.FromMilliseconds(400 * Scale),
            LeaseTtl = TimeSpan.FromSeconds(3 * Scale),
            LeaseRenewSafetyMargin = TimeSpan.FromMilliseconds(700 * Scale),
            OtherPreferenceGraceWindow = TimeSpan.FromMilliseconds(400 * Scale),
            DecisionCacheTtl = TimeSpan.FromSeconds(5 * Scale),
        };
        mutate?.Invoke(options);

        var transport = _hub.CreateTransport();
        var logger = output is null
            ? (ILogger)NullLogger.Instance
            : new TestOutputLogger(output, $"node@{transport.MeshEndpoint.Address}");
        var node = new CoLibraNode(options, logger, TimeProvider.System, transport, udpEngine);
        _endpoints[node] = transport.MeshEndpoint;
        _nodes.Add(node);
        await node.StartAsync(CancellationToken.None);
        if (waitForCluster)
            await node.WaitForClusterAsync().WaitAsync(Eventually);
        return node;
    }

    public void Partition(params CoLibraNode[][] groups) =>
        _hub.Partition([.. groups.Select(g => (IReadOnlyList<IPEndPoint>)[.. g.Select(n => _endpoints[n])])]);

    public void Heal() => _hub.Heal();

    public async Task StopNodeAsync(CoLibraNode node)
    {
        await node.DisposeAsync();
        _nodes.Remove(node);
    }

    public static Task WaitUntilAsync(Func<bool> condition, TimeSpan? timeout = null, string? because = null) =>
        WaitUntilAsync(() => Task.FromResult(condition()), timeout, because);

    public static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan? timeout = null, string? because = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? Eventually);
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
                return;
            await Task.Delay(25);
        }

        Assert.Fail($"Condition not reached within timeout{(because is null ? "" : $": {because}")}");
    }

    public static CoLibraNode? CoordinatorOf(IEnumerable<CoLibraNode> nodes) =>
        nodes.SingleOrDefault(n => n.State == ClusterState.Coordinator);

    public async ValueTask DisposeAsync()
    {
        foreach (var node in _nodes.ToList())
            await node.DisposeAsync();
    }

    private sealed class TestOutputLogger(ITestOutputHelper output, string name) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            try
            {
                output.WriteLine($"{DateTime.UtcNow:HH:mm:ss.fff} [{logLevel}] {name}: {formatter(state, exception)}{(exception is null ? "" : $" | {exception.GetType().Name}: {exception.Message}")}");
            }
            catch
            {
                // test already finished; drop the line
            }
        }
    }
}
