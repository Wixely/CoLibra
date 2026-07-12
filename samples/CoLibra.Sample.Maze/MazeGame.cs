using System.Collections.Concurrent;
using System.Text;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CoLibra.Sample.Maze;

/// <summary>A player's live position, broadcast to everyone on move.</summary>
internal sealed record PlayerState(string Name, int X, int Y);

/// <summary>The map handed to a newly-joined player in response to its request.</summary>
internal sealed record MapResponse(int Width, int Height, string Cells);

/// <summary>
/// The maze game over CoLibra. Discovery finds peers; a lease elects the single map author
/// (first in generates it); newcomers request the map by broadcast and the author replies;
/// positions are broadcast to all with Sequenced delivery (latest-wins). Everyone renders the
/// same maze with all players in 24-bit color.
/// </summary>
internal sealed class MazeGame(
    ICoLibraCluster cluster,
    MazeSettings settings,
    ILogger<MazeGame> logger,
    IHostApplicationLifetime lifetime) : BackgroundService
{
    private const int Cols = 21;
    private const int Rows = 11;

    private readonly ConcurrentDictionary<string, PlayerState> _players = new(StringComparer.Ordinal);
    private Maze? _maze;
    private int _x;
    private int _y;
    private volatile bool _dirty = true;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Player '{Name}' entering the maze...", settings.Name);
        await cluster.WaitForClusterAsync(stoppingToken);

        // Positions from other players (Sequenced: only the newest matters).
        await using var positions = cluster.Messenger.RegisterHandler<PlayerState>("pos", (m, _) =>
        {
            _players[m.Value.Name] = m.Value;
            _dirty = true;
            return ValueTask.CompletedTask;
        });

        // The map author answers "who has the map?" requests with the full map.
        await using var mapRequests = cluster.Messenger.RegisterHandler<string>("map-request", async (m, ct) =>
        {
            if (_maze is { } maze)
                await cluster.Messenger.SendAsync(m.Origin, "map-response", new MapResponse(maze.Width, maze.Height, maze.Cells), cancellationToken: ct);
        });

        var mapReady = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var mapResponses = cluster.Messenger.RegisterHandler<MapResponse>("map-response", (m, _) =>
        {
            _maze ??= new Maze(m.Value.Width, m.Value.Height, m.Value.Cells);
            mapReady.TrySetResult();
            return ValueTask.CompletedTask;
        });

        await BootstrapMapAsync(mapReady, stoppingToken);
        if (_maze is null)
            return; // cancelled during bootstrap (shutdown) — nothing to spawn into

        // Spawn at a random open cell and announce ourselves.
        var random = new Random();
        (_x, _y) = _maze.RandomOpenCell(random);
        _players[settings.Name] = new PlayerState(settings.Name, _x, _y);
        await BroadcastPositionAsync(stoppingToken);
        logger.LogInformation("Spawned at ({X},{Y}) in the {W}x{H} maze", _x, _y, _maze.Width, _maze.Height);

        // Re-announce when anyone joins, so newcomers see us (and reclaim the map if we authored it).
        cluster.MembershipChanged += (_, _) => { _ = BroadcastPositionAsync(stoppingToken); _dirty = true; };
        cluster.LeaseLost += (_, _) => { }; // map-author lease loss is fine; another node re-answers

        var render = RenderLoopAsync(stoppingToken);
        if (Console.IsInputRedirected)
            await RunBotAsync(random, stoppingToken);
        else
            await RunInteractiveAsync(stoppingToken);
        await render.ContinueWith(_ => { }, CancellationToken.None);
    }

    // =====================================================================================
    // Map bootstrap: a lease elects the author; everyone else requests the map by broadcast.
    // =====================================================================================

    private async Task BootstrapMapAsync(TaskCompletionSource mapReady, CancellationToken ct)
    {
        // Exactly one node wins this lease and becomes the canonical map author.
        if (await cluster.CanProcessAsync("maze", "author", ProcessingPreference.This, ct))
        {
            _maze = Maze.Generate(Cols, Rows, Random.Shared.Next());
            logger.LogInformation("You authored the maze ({W}x{H})", _maze.Width, _maze.Height);
            return;
        }

        // Someone else authored it — ask the cluster for the map until it arrives.
        logger.LogInformation("Requesting the maze from the current author...");
        while (!ct.IsCancellationRequested && _maze is null)
        {
            await cluster.Messenger.BroadcastAsync("map-request", settings.Name, cancellationToken: ct);
            var arrived = await Task.WhenAny(mapReady.Task, Task.Delay(TimeSpan.FromMilliseconds(750), ct));
            if (arrived == mapReady.Task)
                break;
        }
    }

    private async Task BroadcastPositionAsync(CancellationToken ct)
    {
        if (_maze is null)
            return;
        try
        {
            await cluster.Messenger.BroadcastAsync("pos", new PlayerState(settings.Name, _x, _y),
                MessageDelivery.Sequenced, ct);
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

                if ((dx, dy) != (0, 0) && !_maze!.IsWall(_x + dx, _y + dy))
                {
                    _x += dx;
                    _y += dy;
                    _players[settings.Name] = new PlayerState(settings.Name, _x, _y);
                    _dirty = true;
                    await BroadcastPositionAsync(ct);
                }
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

    private (int, int) Quit()
    {
        lifetime.StopApplication();
        return (0, 0);
    }

    /// <summary>Headless mode (redirected input / CI): wander randomly so the demo self-drives.</summary>
    private async Task RunBotAsync(Random random, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(400), ct);
                Span<(int, int)> moves = [(0, -1), (0, 1), (-1, 0), (1, 0)];
                var (dx, dy) = moves[random.Next(moves.Length)];
                if (!_maze!.IsWall(_x + dx, _y + dy))
                {
                    _x += dx;
                    _y += dy;
                    _players[settings.Name] = new PlayerState(settings.Name, _x, _y);
                    await BroadcastPositionAsync(ct);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    // =====================================================================================
    // Rendering (24-bit color, UTF-8)
    // =====================================================================================

    private async Task RenderLoopAsync(CancellationToken ct)
    {
        Console.OutputEncoding = Encoding.UTF8;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                if (_dirty && _maze is not null && !Console.IsInputRedirected)
                {
                    _dirty = false;
                    try
                    {
                        Render();
                    }
                    catch (Exception ex)
                    {
                        // A single bad frame must never freeze the console; drop it and keep going.
                        logger.LogDebug(ex, "Render frame failed");
                    }
                }

                await Task.Delay(40, ct); // ~25 fps cap
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private void Render()
    {
        var maze = _maze!;
        // Snapshot player positions by cell for quick lookup.
        var occupants = new Dictionary<(int, int), PlayerState>();
        foreach (var p in _players.Values)
            occupants[(p.X, p.Y)] = p;

        var sb = new StringBuilder(Ansi.Home);
        sb.Append(Ansi.Fg(180, 180, 190)).Append("CoLibra Maze  —  ")
          .Append(_players.Count).Append(" player(s)   [WASD/arrows to move, Q to quit]").Append(Ansi.Reset).Append('\n');

        for (var y = 0; y < maze.Height; y++)
        {
            for (var x = 0; x < maze.Width; x++)
            {
                if (occupants.TryGetValue((x, y), out var who))
                {
                    var (r, g, b) = Ansi.PlayerColor(who.Name);
                    var self = who.Name == settings.Name;
                    sb.Append(Ansi.Fg(r, g, b)).Append(self ? '@' : '●'); // @ = you, ● = others
                }
                else if (maze.IsWall(x, y))
                {
                    // Smooth diagonal hue gradient across the walls (blue → magenta → warm).
                    var hue = 210 + 140.0 * (x + y) / (maze.Width + maze.Height);
                    var (r, g, b) = Ansi.FromHsv(hue, 0.55, 0.85);
                    sb.Append(Ansi.Fg(r, g, b)).Append('█'); // full block
                }
                else
                {
                    sb.Append(Ansi.Fg(40, 44, 52)).Append('·'); // faint middle dot = path
                }
            }

            sb.Append('\n');
        }

        sb.Append(Ansi.Reset);
        // Legend of who's here, each in their color.
        foreach (var p in _players.Values.OrderBy(p => p.Name, StringComparer.Ordinal))
        {
            var (r, g, b) = Ansi.PlayerColor(p.Name);
            sb.Append(Ansi.Fg(r, g, b)).Append(p.Name == settings.Name ? "@ " : "● ").Append(p.Name).Append(Ansi.Reset).Append("  ");
        }

        sb.Append(Ansi.Reset).Append('\n');
        Console.Out.Write(sb.ToString());
    }
}
