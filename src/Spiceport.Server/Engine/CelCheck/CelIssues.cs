using System.Text;

namespace Spiceport.Engine;

/// <summary>One parse or type-check error at a source location.</summary>
internal sealed record CelIssue(CelLocation Location, string Message);

/// <summary>
/// The errors collected while parsing and checking a CEL expression, rendered as cel-go renders an
/// <c>*cel.Issues</c> (<c>common.Errors.ToDisplayString</c>): sorted by location, one
/// <c>ERROR: &lt;source&gt;:&lt;line&gt;:&lt;column&gt;: &lt;message&gt;</c> per error, each followed by
/// the offending source line and a caret under the column.
/// </summary>
internal sealed class CelIssues
{
    private const int MaxErrorsToReport = 100;
    private const int MaxSnippetLength = 16384;

    private readonly List<CelIssue> _errors = [];
    private int _count;

    /// <summary>True when at least one error was reported.</summary>
    public bool HasErrors => _count > 0;

    /// <summary>The reported errors, in report order.</summary>
    public IReadOnlyList<CelIssue> Errors => _errors;

    /// <summary>Records an error (cel-go keeps the first hundred and counts the rest).</summary>
    public void Report(CelLocation location, string message)
    {
        _count++;
        if (_count > MaxErrorsToReport)
            return;
        _errors.Add(new CelIssue(location, message));
    }

    /// <summary>
    /// Renders the errors against <paramref name="sourceText"/>, the expression text cel-go compiled,
    /// described by <paramref name="description"/> (the caveat name). Locations are taken as given; a
    /// caller that relocated them into an enclosing document keeps the snippet of the expression itself,
    /// exactly as SpiceDB does.
    /// </summary>
    public static string ToDisplayString(IReadOnlyList<CelIssue> errors, int totalCount, string sourceText, string description)
    {
        var sorted = errors
            .Select((e, i) => (e, i))
            .OrderBy(p => p.e.Location.Line)
            .ThenBy(p => p.e.Location.Column)
            .ThenBy(p => p.i)
            .Select(p => p.e)
            .ToList();

        var source = new Source(sourceText);
        var lines = sorted.Select(e => Display(e, source, description)).ToList();
        if (totalCount > MaxErrorsToReport)
            lines.Add($"{totalCount - MaxErrorsToReport} more errors were truncated");
        return string.Join("\n", lines);
    }

    /// <summary>Renders these issues against the given source.</summary>
    public string ToDisplayString(string sourceText, string description) =>
        ToDisplayString(_errors, _count, sourceText, description);

    /// <summary>The total number of errors reported, including any beyond the hundred kept.</summary>
    public int Count => _count;

    private static string Display(CelIssue error, Source source, string description)
    {
        var result = new StringBuilder();
        result.Append($"ERROR: {description}:{error.Location.Line}:{error.Location.Column + 1}: {error.Message}");
        if (source.Snippet(error.Location.Line) is { } snippet && Encoding.UTF8.GetByteCount(snippet) <= MaxSnippetLength)
        {
            snippet = snippet.Replace('\t', ' ');
            result.Append("\n | ").Append(snippet);
            result.Append("\n | ");
            var runes = snippet.EnumerateRunes().ToList();
            var index = 0;
            for (var i = 0; i < error.Location.Column && index < runes.Count; i++, index++)
                result.Append(runes[index].Utf8SequenceLength > 1 ? "．" : ".");
            result.Append(index < runes.Count && runes[index].Utf8SequenceLength > 1 ? "＾" : "^");
        }

        return result.ToString();
    }

    /// <summary>cel-go's <c>common.Source</c> line bookkeeping, over code points.</summary>
    private sealed class Source
    {
        private readonly List<Rune> _runes;
        private readonly List<int> _lineOffsets = [];

        public Source(string text)
        {
            _runes = text.EnumerateRunes().ToList();
            if (_runes.Count == 0)
            {
                _lineOffsets.Add(0);
                return;
            }

            for (var offset = 0; offset < _runes.Count; offset++)
            {
                if (_runes[offset].Value == '\n')
                    _lineOffsets.Add(offset + 1);
            }

            _lineOffsets.Add(_runes.Count + 1);
        }

        public string? Snippet(int line)
        {
            if (FindLineOffset(line) is not { } start || _runes.Count == 0)
                return null;
            var end = FindLineOffset(line + 1) is { } next ? next - 1 : _runes.Count;
            return Slice(start, end);
        }

        private int? FindLineOffset(int line)
        {
            if (line == 1)
                return 0;
            if (line > 1 && line <= _lineOffsets.Count)
                return _lineOffsets[line - 2];
            return null;
        }

        private string Slice(int start, int end)
        {
            var text = new StringBuilder();
            for (var i = start; i < end && i < _runes.Count; i++)
                text.Append(_runes[i].ToString());
            return text.ToString();
        }
    }
}
