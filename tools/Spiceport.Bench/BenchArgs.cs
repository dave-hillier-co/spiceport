namespace Spiceport.Bench;

/// <summary>A user-facing argument error: printed without a stack trace, exit code 2.</summary>
public sealed class BenchUsageException(string message) : Exception(message);

/// <summary>
/// Hand-rolled <c>--key=value</c> / <c>--flag</c> parser (no System.CommandLine by design — the
/// harness takes no package dependencies beyond what the cluster itself needs). Every scenario
/// declares its known keys via <see cref="Validate"/> so a typo fails loudly instead of silently
/// running with a default.
/// </summary>
public sealed class BenchArgs
{
    private readonly Dictionary<string, string> _values = new(StringComparer.Ordinal);

    /// <summary>Parses the arguments after the scenario name. Accepts <c>--key=value</c> and bare <c>--flag</c>.</summary>
    public static BenchArgs Parse(IEnumerable<string> args)
    {
        var parsed = new BenchArgs();
        foreach (var raw in args)
        {
            if (!raw.StartsWith("--", StringComparison.Ordinal))
                throw new BenchUsageException($"Unexpected argument '{raw}' (expected --key=value).");
            var body = raw[2..];
            var eq = body.IndexOf('=');
            var (key, value) = eq < 0 ? (body, "") : (body[..eq], body[(eq + 1)..]);
            if (key.Length == 0)
                throw new BenchUsageException($"Malformed argument '{raw}'.");
            if (!parsed._values.TryAdd(key, value))
                throw new BenchUsageException($"Duplicate argument '--{key}'.");
        }
        return parsed;
    }

    /// <summary>Throws if any parsed key is not in <paramref name="known"/> (typo protection).</summary>
    public void Validate(params string[] known)
    {
        var set = new HashSet<string>(known, StringComparer.Ordinal);
        foreach (var key in _values.Keys)
        {
            if (!set.Contains(key))
                throw new BenchUsageException(
                    $"Unknown flag '--{key}'. Known flags: {string.Join(", ", known.Select(k => "--" + k))}.");
        }
    }

    /// <summary>True when the flag was present at all (with or without a value).</summary>
    public bool Has(string name) => _values.ContainsKey(name);

    /// <summary>The flag's raw string value, or <paramref name="fallback"/> when absent.</summary>
    public string? GetString(string name, string? fallback = null) =>
        _values.TryGetValue(name, out var v) ? v : fallback;

    /// <summary>Integer flag; throws <see cref="BenchUsageException"/> on a non-integer value.</summary>
    public int GetInt(string name, int fallback) =>
        _values.TryGetValue(name, out var v)
            ? int.TryParse(v, out var i) ? i : throw new BenchUsageException($"--{name} expects an integer, got '{v}'.")
            : fallback;

    /// <summary>Floating-point flag; throws <see cref="BenchUsageException"/> on a non-numeric value.</summary>
    public double GetDouble(string name, double fallback) =>
        _values.TryGetValue(name, out var v)
            ? double.TryParse(v, out var d) ? d : throw new BenchUsageException($"--{name} expects a number, got '{v}'.")
            : fallback;

    /// <summary>Comma-separated integer list, e.g. <c>--sizes=1000,10000</c>.</summary>
    public int[] GetIntArray(string name, int[] fallback)
    {
        if (!_values.TryGetValue(name, out var v))
            return fallback;
        try
        {
            return v.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Select(int.Parse).ToArray();
        }
        catch (FormatException)
        {
            throw new BenchUsageException($"--{name} expects comma-separated integers, got '{v}'.");
        }
    }

    /// <summary>Comma-separated string list, e.g. <c>--breadths=none,single,type</c>.</summary>
    public string[] GetStringArray(string name, string[] fallback) =>
        _values.TryGetValue(name, out var v)
            ? v.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            : fallback;

    /// <summary>
    /// Duration flag: <c>500ms</c>, <c>5s</c>, <c>2m</c>, <c>1h</c>, or a bare number meaning seconds.
    /// </summary>
    public TimeSpan GetDuration(string name, TimeSpan fallback) =>
        _values.TryGetValue(name, out var v) ? ParseDuration(name, v) : fallback;

    /// <summary>Comma-separated durations, e.g. <c>--windows=1s,5s,30s</c>.</summary>
    public TimeSpan[] GetDurationArray(string name, TimeSpan[] fallback) =>
        _values.TryGetValue(name, out var v)
            ? v.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Select(item => ParseDuration(name, item)).ToArray()
            : fallback;

    private static TimeSpan ParseDuration(string name, string v)
    {
        var text = v.Trim();
        var (number, perUnitSeconds) =
            text.EndsWith("ms", StringComparison.Ordinal) ? (text[..^2], 0.001)
            : text.EndsWith("s", StringComparison.Ordinal) ? (text[..^1], 1.0)
            : text.EndsWith("m", StringComparison.Ordinal) ? (text[..^1], 60.0)
            : text.EndsWith("h", StringComparison.Ordinal) ? (text[..^1], 3600.0)
            : (text, 1.0);
        if (!double.TryParse(number, out var n))
            throw new BenchUsageException($"--{name} expects a duration like 5s/2m/500ms, got '{v}'.");
        return TimeSpan.FromSeconds(n * perUnitSeconds);
    }
}
