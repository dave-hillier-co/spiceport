namespace Spiceport.Schema;

/// <summary>
/// Thrown when the schema DSL cannot be lexed, parsed, or compiled.
/// </summary>
public sealed class SchemaCompileException : Exception
{
    /// <summary>The 1-based line where the error occurred (0 if unknown).</summary>
    public int Line { get; }

    /// <summary>The 1-based column where the error occurred (0 if unknown).</summary>
    public int Column { get; }

    public SchemaCompileException(string message, int line = 0, int column = 0)
        : base(Format(message, line, column))
    {
        Line = line;
        Column = column;
    }

    private static string Format(string message, int line, int column) =>
        line > 0 ? $"schema error at line {line}, column {column}: {message}" : $"schema error: {message}";
}
