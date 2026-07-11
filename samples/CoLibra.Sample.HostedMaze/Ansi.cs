namespace CoLibra.Sample.HostedMaze;

/// <summary>24-bit (truecolor) ANSI helpers and HSV→RGB for smooth gradients.</summary>
internal static class Ansi
{
    public const string Reset = "\x1b[0m";
    public const string Home = "\x1b[H";
    public const string Clear = "\x1b[2J\x1b[H";
    public const string HideCursor = "\x1b[?25l";
    public const string ShowCursor = "\x1b[?25h";

    public static string Fg(byte r, byte g, byte b) => $"\x1b[38;2;{r};{g};{b}m";

    public static (byte R, byte G, byte B) FromHsv(double h, double s, double v)
    {
        h = ((h % 360) + 360) % 360;
        var c = v * s;
        var x = c * (1 - Math.Abs(h / 60 % 2 - 1));
        var m = v - c;
        var (r, g, b) = h switch
        {
            < 60 => (c, x, 0.0),
            < 120 => (x, c, 0.0),
            < 180 => (0.0, c, x),
            < 240 => (0.0, x, c),
            < 300 => (x, 0.0, c),
            _ => (c, 0.0, x),
        };
        return ((byte)((r + m) * 255), (byte)((g + m) * 255), (byte)((b + m) * 255));
    }

    public static (byte R, byte G, byte B) PlayerColor(string name)
    {
        var hash = 0;
        foreach (var ch in name)
            hash = hash * 31 + ch;
        return FromHsv(Math.Abs(hash) % 360, 0.85, 1.0);
    }
}
