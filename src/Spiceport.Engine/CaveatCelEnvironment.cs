using System.Net;
using Cel;

namespace Spiceport.Engine;

/// <summary>
/// Builds the shared CEL environment used for both compiling (parse-validating at schema write) and
/// evaluating SpiceDB-style caveats. Registers the SpiceDB custom functions/types: <c>ipaddress(string)</c>
/// with <c>.in_cidr(string)</c>, and a map <c>.isSubtreeOf(map)</c> structural-subtree check.
/// <c>timestamp</c>/<c>duration</c> are provided by the underlying CEL implementation.
/// </summary>
internal static class CaveatCelEnvironment
{
    /// <summary>Creates a fresh environment with the SpiceDB caveat functions registered.</summary>
    public static CelEnvironment Build()
    {
        var env = new CelEnvironment(null, null);

        // ipaddress(string) -> the string itself (represented as a string in this engine).
        env.RegisterFunction("ipaddress", [typeof(string)], args => args[0]);

        // <ipaddress|string>.in_cidr(cidr_string) -> bool. A null receiver/argument means a referenced
        // variable was absent; signal it as a missing reference so callers return Caveated.
        env.RegisterFunction("in_cidr", [typeof(object), typeof(string)], args =>
        {
            RequireBound(args[0]);
            RequireBound(args[1]);
            return InCidr(AsString(args[0]), AsString(args[1]));
        });

        // <map>.isSubtreeOf(other_map) -> bool
        env.RegisterFunction("isSubtreeOf", [typeof(object), typeof(object)], args =>
        {
            RequireBound(args[0]);
            RequireBound(args[1]);
            return IsSubtreeOf(args[0]!, args[1]!);
        });

        return env;
    }

    /// <summary>
    /// Throws <see cref="CelUndeclaredReferenceException"/> when a custom-function argument is null,
    /// which (given present-but-null context values are stripped before evaluation) can only mean a
    /// referenced variable was absent.
    /// </summary>
    private static void RequireBound(object? arg)
    {
        if (arg is null)
            throw new CelUndeclaredReferenceException("a referenced caveat parameter was not supplied");
    }

    private static string AsString(object? o) => o as string ?? o?.ToString() ?? string.Empty;

    /// <summary>Returns true if <paramref name="ip"/> is contained within the CIDR network.</summary>
    private static bool InCidr(string ip, string cidr)
    {
        if (!IPAddress.TryParse(ip, out var addr))
            return false;

        var slash = cidr.IndexOf('/');
        if (slash < 0)
            return IPAddress.TryParse(cidr, out var single) && single.Equals(addr);

        if (!IPAddress.TryParse(cidr[..slash], out var network)
            || !int.TryParse(cidr[(slash + 1)..], out var prefixLen))
            return false;

        if (addr.AddressFamily != network.AddressFamily)
            return false;

        var addrBytes = addr.GetAddressBytes();
        var netBytes = network.GetAddressBytes();
        if (addrBytes.Length != netBytes.Length)
            return false;

        var totalBits = addrBytes.Length * 8;
        if (prefixLen < 0 || prefixLen > totalBits)
            return false;

        for (var bit = 0; bit < prefixLen; bit++)
        {
            var byteIndex = bit / 8;
            var mask = (byte)(1 << (7 - (bit % 8)));
            if ((addrBytes[byteIndex] & mask) != (netBytes[byteIndex] & mask))
                return false;
        }

        return true;
    }

    /// <summary>
    /// Returns true if <paramref name="left"/> is a structural subtree of <paramref name="right"/>:
    /// every key in left exists in right with an equal value (recursively for nested maps).
    /// </summary>
    private static bool IsSubtreeOf(object left, object right)
    {
        if (left is not IDictionary<string, object> l || right is not IDictionary<string, object> r)
            return false;

        foreach (var (key, lv) in l)
        {
            if (!r.TryGetValue(key, out var rv))
                return false;

            if (lv is IDictionary<string, object> && rv is IDictionary<string, object>)
            {
                if (!IsSubtreeOf(lv, rv))
                    return false;
            }
            else if (!ValuesEqual(lv, rv))
            {
                return false;
            }
        }

        return true;
    }

    private static bool ValuesEqual(object? a, object? b)
    {
        if (a is null || b is null)
            return Equals(a, b);
        if (a is IConvertible && b is IConvertible && IsNumeric(a) && IsNumeric(b))
            return Convert.ToDouble(a).Equals(Convert.ToDouble(b));
        return a.Equals(b);
    }

    private static bool IsNumeric(object o) =>
        o is sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal
            or System.Numerics.BigInteger;
}
