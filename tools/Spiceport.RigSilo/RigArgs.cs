namespace Spiceport.RigSilo;

/// <summary>A user-facing argument error: printed without a stack trace, exit code 2.</summary>
public sealed class RigUsageException(string message) : Exception(message);

/// <summary>
/// Hand-rolled <c>--key=value</c> parser mirroring <c>tools/Spiceport.Bench/BenchArgs.cs</c>'s
/// ergonomics (no System.CommandLine dependency). The rig is an orchestrator-driven process, not an
/// interactive tool, so every flag this type reads is required — a missing flag is a launch-config bug
/// that must fail loud, not silently default into a port collision with a sibling silo.
/// </summary>
public sealed class RigArgs
{
    private readonly Dictionary<string, string> _values = new(StringComparer.Ordinal);

    public static RigArgs Parse(IEnumerable<string> args)
    {
        var parsed = new RigArgs();
        foreach (var raw in args)
        {
            if (!raw.StartsWith("--", StringComparison.Ordinal))
                throw new RigUsageException($"Unexpected argument '{raw}' (expected --key=value).");
            var body = raw[2..];
            var eq = body.IndexOf('=');
            var (key, value) = eq < 0 ? (body, "") : (body[..eq], body[(eq + 1)..]);
            if (key.Length == 0)
                throw new RigUsageException($"Malformed argument '{raw}'.");
            if (!parsed._values.TryAdd(key, value))
                throw new RigUsageException($"Duplicate argument '--{key}'.");
        }
        return parsed;
    }

    public string GetString(string name, string? fallback = null) =>
        _values.TryGetValue(name, out var v) ? v
            : fallback ?? throw new RigUsageException($"Missing required flag --{name}.");

    public int GetRequiredInt(string name) =>
        _values.TryGetValue(name, out var v)
            ? int.TryParse(v, out var i) ? i : throw new RigUsageException($"--{name} expects an integer, got '{v}'.")
            : throw new RigUsageException($"Missing required flag --{name}.");
}
