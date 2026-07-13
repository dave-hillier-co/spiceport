namespace Spiceport.Conformance.Tests;

/// <summary>
/// Makes quarantined corpus files visible in every test run, not just in
/// <c>TestData/Quarantine/README.md</c>. Every <c>*.yaml</c> under <c>TestData/Quarantine/</c> gets
/// one explicitly-skipped test case; the skip reason is the documented quarantine reason so a
/// reviewer sees the gap without opening the README. This folder is deliberately excluded from
/// the main corpus loaders (<see cref="ConformanceTests"/>, <see cref="ValidationBlockTests"/>,
/// <see cref="ReverseConsistencyCrossCheckTests"/>), which only enumerate <c>TestData/*.yaml</c>
/// non-recursively — quarantined files are never silently picked up as part of the 73+ main corpus.
/// </summary>
public class QuarantinedCorpusTests
{
    /// <summary>
    /// Per-file quarantine reasons, keyed by filename. Kept alongside (and in sync with)
    /// <c>TestData/Quarantine/README.md</c>; empty because nothing is currently quarantined.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, string> QuarantineReasons =
        new Dictionary<string, string>(StringComparer.Ordinal);

    /// <summary>No-quarantine sentinel: an empty MemberData source is itself an xUnit failure
    /// ("No data found"/wrong parameter count), so the (expected, documented) zero-files case
    /// is represented as one explicit row instead of zero rows.</summary>
    private const string NoneQuarantinedSentinel = "";

    public static TheoryData<string> QuarantinedFiles()
    {
        var data = new TheoryData<string>();
        var dir = Path.Combine(AppContext.BaseDirectory, "TestData", "Quarantine");
        var files = Directory.Exists(dir)
            ? Directory.EnumerateFiles(dir, "*.yaml").OrderBy(p => p, StringComparer.Ordinal).Select(Path.GetFileName)
            : [];

        var any = false;
        foreach (var file in files)
        {
            data.Add(file!);
            any = true;
        }

        if (!any)
        {
            data.Add(NoneQuarantinedSentinel);
        }

        return data;
    }

    /// <summary>Reports one skipped test per quarantined file (or a trivial pass when none exist).</summary>
    [SkippableTheory]
    [MemberData(nameof(QuarantinedFiles))]
    public void Quarantined_file_is_reported_as_skipped(string fileName)
    {
        if (fileName == NoneQuarantinedSentinel)
        {
            return; // Nothing is currently quarantined; nothing to skip-report.
        }

        var reason = QuarantineReasons.TryGetValue(fileName, out var r)
            ? r
            : "quarantined (see TestData/Quarantine/README.md for the recorded reason)";

        Skip.If(true, $"{fileName}: {reason}");
    }
}
