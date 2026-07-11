using System.Text;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CoLibra.Sample.HostedMaze;

// Wire messages between clients and the current host.
internal sealed record Join(string Name);
internal sealed record Move(string Name, int Dx, int Dy);
internal sealed record PlayerPos(string Name, int X, int Y);
internal sealed record Snapshot(int Width, int Height, string Cells, IReadOnlyList<PlayerPos> Players);

/// <summary>
/// A maze where the CoLibra COORDINATOR is the authoritative game host: it owns the maze and
/// every player's position, applies moves, and broadcasts the full state. Clients send their
/// moves to the host and render what it broadcasts. When the host disconnects, CoLibra elects
/// a new coordinator automatically — and because every client already holds the last full
/// snapshot, whoever is elected simply promotes it to authoritative and keeps hosting. No
/// dedicated server, no lobby, no lost state: the crown just moves to another player.
/// </summary>
internal sealed class HostedMazeGame(
    ICoLibraCluster cluster,
    HostedMazeSettings settings,
    ILogger<HostedMazeGame> logger,
    IHostApplicationLifetime lifetime) : BackgroundService
{
    private const int Cols = 21;
    private const int Rows = 11;
    private static readonly TimeSpan Tick = TimeSpan.FromMilliseconds(140);

    private readonly Lock _gate = new();
    private readonly Dictionary<string, PlayerPos> _positions = new(StringComparer.Ordinal);
    private readonly Random _random = new();
    private Maze? _maze;
    private volatile bool _dirty = true;

    private bool IsHost => cluster.State == ClusterState.Coordinator;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        cluster.StateChanged += (_, e) =>
        {
            if (e.Current == ClusterState.Coordinator)
                OnBecameHost();
            _dirty = true;
        };
        cluster.MembershipChanged += (_, e) =>
        {
            if (IsHost)
            {
                lock (_gate)
                {
                    foreach (var left in e.Left)
                        _positions.Remove(NameForLeft(e, left));
                }
            }

            _dirty = true;
        };

        logger.LogInformation("'{Name}' joining the arena...", settings.Name);
        await cluster.WaitForClusterAsync(stoppingToken);

        // Host handlers: apply authoritative changes.
        await using var joins = cluster.Messenger.RegisterHandler<Join>("join", (m, _) =>
        {
            if (IsHost)
                EnsureSpawned(m.Value.Name);
            return ValueTask.CompletedTask;
        });
        await using var moves = cluster.Messenger.RegisterHandler<Move>("move", (m, _) =>
        {
            if (IsHost)
                ApplyMove(m.Value.Name, m.Value.Dx, m.Value.Dy);
            return ValueTask.CompletedTask;
        });
        // Client handler: adopt the host's broadcast state.
        await using var snapshots = cluster.Messenger.RegisterHandler<Snapshot>("snapshot", (m, _) =>
        {
            if (!IsHost)
                AdoptSnapshot(m.Value);
            return ValueTask.CompletedTask;
        });

        if (IsHost)
            OnBecameHost();

        var host = HostLoopAsync(stoppingToken);
        var client = ClientLoopAsync(stoppingToken);
        var render = RenderLoopAsync(stoppingToken);
        if (Console.IsInputRedirected)
            await RunBotAsync(stoppingToken);
        else
            await RunInteractiveAsync(stoppingToken);
        await Task.WhenAll(
            host.ContinueWith(_ => { }, CancellationToken.None),
            client.ContinueWith(_ => { }, CancellationToken.None),
            render.ContinueWith(_ => { }, CancellationToken.None));
    }

    private static string NameForLeft(MembershipChangedEventArgs e, NodeId left) =>
        e.Members.FirstOrDefault(m => m.NodeId == left)?.Name ?? left.ToString();

    // =====================================================================================
    // Host: owns the maze + positions, broadcasts the full snapshot each tick.
    // =====================================================================================

    private void OnBecameHost()
    {
        lock (_gate)
        {
            // The maze survived in the last snapshot we adopted; only the very first host makes one.
            _maze ??= Maze.Generate(Cols, Rows, _random.Next());
            EnsureSpawnedLocked(settings.Name);
        }

        logger.LogInformation("You are now the HOST (authoritative game server)");
    }

    private async Task HostLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(Tick, ct);
                if (!IsHost)
                    continue;

                Snapshot snapshot;
                lock (_gate)
                {
                    if (_maze is not { } maze)
                        continue;
                    snapshot = new Snapshot(maze.Width, maze.Height, maze.Cells, [.. _positions.Values]);
                }

                _dirty = true; // the host renders from its own authoritative state
                await cluster.Messenger.BroadcastAsync("snapshot", snapshot, MessageDelivery.Sequenced, ct);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private void EnsureSpawned(string name)
    {
        lock (_gate)
            EnsureSpawnedLocked(name);
    }

    private void EnsureSpawnedLocked(string name)
    {
        if (_maze is { } maze && !_positions.ContainsKey(name))
        {
            var (x, y) = maze.RandomOpenCell(_random);
            _positions[name] = new PlayerPos(name, x, y);
            _dirty = true;
        }
    }

    private void ApplyMove(string name, int dx, int dy)
    {
        lock (_gate)
        {
            if (_maze is not { } maze || !_positions.TryGetValue(name, out var p))
                return;
            var (nx, ny) = (p.X + dx, p.Y + dy);
            if (!maze.IsWall(nx, ny))
            {
                _positions[name] = new PlayerPos(name, nx, ny);
                _dirty = true;
            }
        }
    }

    // =====================================================================================
    // Client: send moves to the host, render what the host broadcasts.
    // =====================================================================================

    private void AdoptSnapshot(Snapshot snapshot)
    {
        lock (_gate)
        {
            _maze = new Maze(snapshot.Width, snapshot.Height, snapshot.Cells);
            _positions.Clear();
            foreach (var p in snapshot.Players)
                _positions[p.Name] = p;
        }

        _dirty = true;
    }

    private async Task ClientLoopAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(1), ct);
                // While a member, make sure the current host knows about us (covers joining and
                // re-registering with a freshly-elected host after a migration).
                bool present;
                lock (_gate)
                    present = _positions.ContainsKey(settings.Name);
                if (!IsHost && !present)
                    await SendToHostAsync("join", new Join(settings.Name), ct);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task SendToHostAsync<T>(string channel, T value, CancellationToken ct)
    {
        var host = cluster.Members.FirstOrDefault(m => m.IsCoordinator);
        if (host is null)
            return;
        try
        {
            await cluster.Messenger.SendAsync(host.NodeId, channel, value, MessageDelivery.Sequenced, ct);
        }
        catch (OperationCanceledException)
        {
        }
    }

    // =====================================================================================
    // Input
    // =====================================================================================

    private async Task RunInteractiveAsync(CancellationToken ct)
    {
        Console.Write(Ansi.HideCursor + Ansi.Clear);
        try
        {
            while (!ct.IsCancellationRequested)
            {
                if (!Console.KeyAvailable)
                {
                    await Task.Delay(15, ct);
                    continue;
                }

                var (dx, dy) = Console.ReadKey(intercept: true).Key switch
                {
                    ConsoleKey.W or ConsoleKey.UpArrow => (0, -1),
                    ConsoleKey.S or ConsoleKey.DownArrow => (0, 1),
                    ConsoleKey.A or ConsoleKey.LeftArrow => (-1, 0),
                    ConsoleKey.D or ConsoleKey.RightArrow => (1, 0),
                    ConsoleKey.Q => Quit(),
                    _ => (0, 0),
                };

                if ((dx, dy) != (0, 0))
                    await StepAsync(dx, dy, ct);
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            Console.Write(Ansi.ShowCursor + Ansi.Reset);
        }
    }

    private async Task StepAsync(int dx, int dy, CancellationToken ct)
    {
        if (IsHost)
        {
            ApplyMove(settings.Name, dx, dy); // the host applies its own move authoritatively
        }
        else
        {
            // Optimistic local move for snappiness; the next authoritative snapshot reconciles.
            lock (_gate)
            {
                if (_maze is { } maze && _positions.TryGetValue(settings.Name, out var p) && !maze.IsWall(p.X + dx, p.Y + dy))
                {
                    _positions[settings.Name] = new PlayerPos(settings.Name, p.X + dx, p.Y + dy);
                    _dirty = true;
                }
            }

            await SendToHostAsync("move", new Move(settings.Name, dx, dy), ct);
        }
    }

    private (int, int) Quit()
    {
        lifetime.StopApplication();
        return (0, 0);
    }

    private async Task RunBotAsync(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(350), ct);
                Span<(int, int)> moves = [(0, -1), (0, 1), (-1, 0), (1, 0)];
                var (dx, dy) = moves[_random.Next(moves.Length)];
                await StepAsync(dx, dy, ct);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    // =====================================================================================
    // Rendering (24-bit color, UTF-8). The host wears a crown; migration moves it visibly.
    // =====================================================================================

    private async Task RenderLoopAsync(CancellationToken ct)
    {
        Console.OutputEncoding = Encoding.UTF8;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                if (_dirty && !Console.IsInputRedirected)
                {
                    _dirty = false;
                    Render();
                }

                await Task.Delay(40, ct);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private void Render()
    {
        Maze? maze;
        List<PlayerPos> players;
        lock (_gate)
        {
            maze = _maze;
            players = [.. _positions.Values];
        }

        if (maze is null)
            return;

        var hostName = cluster.Members.FirstOrDefault(m => m.IsCoordinator)?.Name;
        var occupants = players.ToDictionary(p => (p.X, p.Y), p => p);

        var sb = new StringBuilder(Ansi.Home);
        sb.Append(Ansi.Fg(180, 180, 190))
          .Append("CoLibra Hosted Maze  —  host: ").Append(Ansi.Fg(255, 215, 0)).Append("♔ ").Append(hostName ?? "(electing…)")
          .Append(Ansi.Fg(180, 180, 190)).Append(IsHost ? "  (that's you)" : "")
          .Append("   [WASD/arrows, Q quits]").Append(Ansi.Reset).Append('\n');

        for (var y = 0; y < maze.Height; y++)
        {
            for (var x = 0; x < maze.Width; x++)
            {
                if (occupants.TryGetValue((x, y), out var who))
                {
                    var (r, g, b) = Ansi.PlayerColor(who.Name);
                    var isHost = who.Name == hostName;
                    var self = who.Name == settings.Name;
                    sb.Append(Ansi.Fg(r, g, b)).Append(isHost ? '♔' : self ? '@' : '●');
                }
                else if (maze.IsWall(x, y))
                {
                    var hue = 210 + 140.0 * (x + y) / (maze.Width + maze.Height);
                    var (r, g, b) = Ansi.FromHsv(hue, 0.55, 0.85);
                    sb.Append(Ansi.Fg(r, g, b)).Append('█');
                }
                else
                {
                    sb.Append(Ansi.Fg(40, 44, 52)).Append('·');
                }
            }

            sb.Append('\n');
        }

        foreach (var p in players.OrderBy(p => p.Name, StringComparer.Ordinal))
        {
            var (r, g, b) = Ansi.PlayerColor(p.Name);
            var marker = p.Name == hostName ? "♔ " : p.Name == settings.Name ? "@ " : "● ";
            sb.Append(Ansi.Fg(r, g, b)).Append(marker).Append(p.Name).Append(Ansi.Reset).Append("  ");
        }

        sb.Append(Ansi.Reset).Append('\n');
        Console.Out.Write(sb.ToString());
    }
}
