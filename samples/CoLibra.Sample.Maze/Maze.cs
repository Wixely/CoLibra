using System.Text;

namespace CoLibra.Sample.Maze;

/// <summary>
/// A perfect maze on an odd-sized char grid ('#' wall, ' ' open). Generated once by the first
/// player (recursive-backtracker from a random seed) and shipped verbatim to everyone else, so
/// all clients render the exact same map. Serialized as width/height + a flat wall string.
/// </summary>
internal sealed record Maze(int Width, int Height, string Cells)
{
    public bool IsWall(int x, int y) =>
        x < 0 || y < 0 || x >= Width || y >= Height || Cells[y * Width + x] == '#';

    public static Maze Generate(int cols, int rows, int seed)
    {
        // Cell grid expands to a wall grid of (2*cols+1) x (2*rows+1).
        var width = 2 * cols + 1;
        var height = 2 * rows + 1;
        var grid = new char[width * height];
        Array.Fill(grid, '#');

        var random = new Random(seed);
        var visited = new bool[cols * rows];
        var stack = new Stack<(int Cx, int Cy)>();
        stack.Push((0, 0));
        visited[0] = true;
        grid[(2 * 0 + 1) * width + (2 * 0 + 1)] = ' ';

        Span<(int Dx, int Dy)> dirs = [(0, -1), (0, 1), (-1, 0), (1, 0)];
        while (stack.Count > 0)
        {
            var (cx, cy) = stack.Peek();
            // Shuffle directions for this cell.
            for (var i = dirs.Length - 1; i > 0; i--)
            {
                var j = random.Next(i + 1);
                (dirs[i], dirs[j]) = (dirs[j], dirs[i]);
            }

            var carved = false;
            foreach (var (dx, dy) in dirs)
            {
                var (nx, ny) = (cx + dx, cy + dy);
                if (nx < 0 || ny < 0 || nx >= cols || ny >= rows || visited[ny * cols + nx])
                    continue;

                visited[ny * cols + nx] = true;
                // Open the target cell and the wall between it and the current cell.
                grid[(2 * ny + 1) * width + (2 * nx + 1)] = ' ';
                grid[(2 * cy + 1 + dy) * width + (2 * cx + 1 + dx)] = ' ';
                stack.Push((nx, ny));
                carved = true;
                break;
            }

            if (!carved)
                stack.Pop();
        }

        return new Maze(width, height, new string(grid));
    }

    /// <summary>A random open cell (for spawning a player).</summary>
    public (int X, int Y) RandomOpenCell(Random random)
    {
        while (true)
        {
            var x = random.Next(Width);
            var y = random.Next(Height);
            if (!IsWall(x, y))
                return (x, y);
        }
    }
}
