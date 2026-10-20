namespace Spiceport.Engine;

/// <summary>
/// Thrown by <see cref="CaveatCompiler"/> and <see cref="CaveatParameterTypes"/> when a caveat does not
/// compile: an invalid parameter type, a CEL syntax or type error, or a non-boolean expression. The
/// message is SpiceDB's (<c>pkg/caveats/compile.go</c>, <c>pkg/caveats/types/typeset.go</c>), including
/// cel-go's rendering of its issues.
/// </summary>
public sealed class CaveatCompileException(string message) : Exception(message);
