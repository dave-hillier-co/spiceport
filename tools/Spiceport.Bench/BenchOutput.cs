using System.Text.Json;

namespace Spiceport.Bench;

/// <summary>Aligned-column plain-text table printer (no color, no box drawing).</summary>
public sealed class ConsoleTable(params string[] headers)
{
    private readonly List<string[]> _rows = [];

    public void AddRow(params object?[] cells) =>
        _rows.Add(cells.Select(c => c?.ToString() ?? "").ToArray());

    public void Print(TextWriter writer)
    {
        var widths = headers.Select(h => h.Length).ToArray();
        foreach (var row in _rows)
            for (var i = 0; i < widths.Length && i < row.Length; i++)
                widths[i] = Math.Max(widths[i], row[i].Length);

        void PrintRow(IReadOnlyList<string> cells)
        {
            var padded = cells.Select((c, i) => c.PadRight(widths[Math.Min(i, widths.Length - 1)]));
            writer.WriteLine(string.Join("  ", padded).TrimEnd());
        }

        PrintRow(headers);
        PrintRow(widths.Select(w => new string('-', w)).ToArray());
        foreach (var row in _rows)
            PrintRow(row);
    }
}

/// <summary>
/// The <c>--json</c> output path handling, enforcing the program's standing rule: benchmark results
/// are never committed (docs/scalability-program.md, "Invariants that bound the program"), so a
/// results file inside the repository tree is refused outright.
/// </summary>
public static class BenchJson
{
    /// <summary>
    /// Resolves <c>--json</c> to a full path, or null when the flag is absent. Throws
    /// <see cref="BenchUsageException"/> when the path lands inside the repository tree.
    /// </summary>
    public static string? ResolveOutputPath(BenchArgs args)
    {
        var raw = args.GetString("json");
        if (raw is null)
            return null;
        if (raw.Length == 0)
            throw new BenchUsageException("--json requires a path, e.g. --json=/tmp/bench.json.");

        var full = Path.GetFullPath(raw);
        var repoRoot = FindRepoRoot();
        if (repoRoot is not null &&
            full.StartsWith(repoRoot + Path.DirectorySeparatorChar, StringComparison.Ordinal))
        {
            throw new BenchUsageException(
                $"--json path '{full}' is inside the repository tree ({repoRoot}). " +
                "Benchmark results are never committed (docs/scalability-program.md invariant); " +
                "write them outside the repo, e.g. --json=/tmp/bench.json.");
        }
        return full;
    }

    /// <summary>Writes the payload as indented JSON (System.Text.Json — no extra package).</summary>
    public static void Write(string path, object payload)
    {
        var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true });
        File.WriteAllText(path, json);
        Console.Error.WriteLine($"json results written to {path}");
    }

    /// <summary>
    /// Walks up from both the working directory and the binary location looking for the repo marker
    /// (<c>.git</c> or <c>Spiceport.slnx</c>); returns the first hit or null when run from elsewhere.
    /// </summary>
    private static string? FindRepoRoot()
    {
        foreach (var start in new[] { Directory.GetCurrentDirectory(), AppContext.BaseDirectory })
        {
            for (var dir = new DirectoryInfo(start); dir is not null; dir = dir.Parent)
            {
                if (Directory.Exists(Path.Combine(dir.FullName, ".git")) ||
                    File.Exists(Path.Combine(dir.FullName, "Spiceport.slnx")))
                    return dir.FullName;
            }
        }
        return null;
    }
}
