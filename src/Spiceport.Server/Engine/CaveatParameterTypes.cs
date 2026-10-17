using Spiceport.Core;

namespace Spiceport.Engine;

/// <summary>
/// SpiceDB's caveat parameter type system (<c>pkg/caveats/types</c>: <c>basic.go</c>, <c>ipaddress.go</c>,
/// <c>typeset.go</c>): the keywords a caveat parameter may be declared with, and the CEL type each one
/// declares its variable as when the caveat expression is checked.
/// </summary>
/// <remarks>
/// As upstream: <c>uint</c> is declared as CEL <c>int</c> (so <c>x == 1u</c> does not type-check for a
/// <c>uint</c> parameter); <c>any</c> is <c>dyn</c>; <c>map&lt;T&gt;</c> takes ONE generic, the value type,
/// keys always being strings; <c>ipaddress</c> is the opaque object type <c>IPAddress</c>; and a basic
/// type ignores any generics it is given (<c>int&lt;string&gt;</c> is <c>int</c>).
/// </remarks>
public static class CaveatParameterTypes
{
    /// <summary>
    /// Throws a <see cref="CaveatCompileException"/> with SpiceDB's message (<c>unknown type `foo`</c>,
    /// <c>type `list` requires 1 generic types; found 0</c>) if the reference is not a valid parameter type.
    /// </summary>
    public static void Validate(CaveatTypeReference type)
    {
        ArgumentNullException.ThrowIfNull(type);
        ToCelType(type);
    }

    /// <summary>The CEL type a parameter of the given declared type is checked as.</summary>
    internal static CheckedType ToCelType(CaveatTypeReference type)
    {
        var children = (type.ChildTypes ?? []).Select(ToCelType).ToList();
        switch (type.TypeName)
        {
            case "any":
                return CheckedType.Dyn;
            case "bool":
                return CheckedType.Bool;
            case "string":
                return CheckedType.String;
            case "int":
            case "uint":
                return CheckedType.Int;
            case "double":
                return CheckedType.Double;
            case "bytes":
                return CheckedType.Bytes;
            case "duration":
                return CheckedType.Duration;
            case "timestamp":
                return CheckedType.Timestamp;
            case "ipaddress":
                return CelDeclarations.IPAddress;
            case "list":
                RequireGenerics(type.TypeName, children.Count);
                return CheckedType.List(children[0]);
            case "map":
                RequireGenerics(type.TypeName, children.Count);
                return CheckedType.Map(CheckedType.String, children[0]);
            default:
                throw new CaveatCompileException($"unknown type `{type.TypeName}`");
        }
    }

    private static void RequireGenerics(string keyword, int found)
    {
        if (found != 1)
            throw new CaveatCompileException($"type `{keyword}` requires 1 generic types; found {found}");
    }
}
