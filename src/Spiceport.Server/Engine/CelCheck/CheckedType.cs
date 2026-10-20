namespace Spiceport.Engine;

/// <summary>The general category of a <see cref="CheckedType"/>. Mirrors cel-go's <c>types.Kind</c>.</summary>
internal enum CheckedKind
{
    Unspecified,
    Dyn,
    Any,
    Bool,
    Bytes,
    Double,
    Duration,
    Error,
    Int,
    List,
    Map,
    Null,
    Opaque,
    String,
    Struct,
    Timestamp,
    Type,
    TypeParam,
    Uint,
}

/// <summary>
/// A type as the static CEL checker (<see cref="CelTypeChecker"/>) sees it. A transliteration of the
/// type-check half of cel-go's <c>common/types.Type</c> (the <c>authzed/cel-go</c> fork SpiceDB pins):
/// a kind, a runtime type name, and type parameters (a list's element type, a map's key and value types,
/// an opaque type's arguments, a type-of-type's subject).
/// </summary>
/// <remarks>
/// cel-go compares some types by pointer: <c>caveats.CompileCaveatWithSource</c> accepts a caveat only
/// when <c>ast.OutputType() == cel.BoolType</c>, so a type that is merely <i>shaped</i> like bool (a
/// nullable <c>google.protobuf.BoolValue</c> wrapper) is rejected. The singletons below play the part of
/// cel-go's package-level type values, and the checker never constructs a second instance of them, so
/// reference equality keeps that meaning.
/// </remarks>
internal sealed class CheckedType
{
    private readonly CheckedType? _nullableOf;

    private CheckedType(CheckedKind kind, string name, IReadOnlyList<CheckedType> parameters, CheckedType? nullableOf = null)
    {
        Kind = kind;
        Name = name;
        Parameters = parameters;
        _nullableOf = nullableOf;
    }

    public static readonly CheckedType Any = new(CheckedKind.Any, "google.protobuf.Any", []);
    public static readonly CheckedType Bool = new(CheckedKind.Bool, "bool", []);
    public static readonly CheckedType Bytes = new(CheckedKind.Bytes, "bytes", []);
    public static readonly CheckedType Double = new(CheckedKind.Double, "double", []);
    public static readonly CheckedType Duration = new(CheckedKind.Duration, "google.protobuf.Duration", []);
    public static readonly CheckedType Dyn = new(CheckedKind.Dyn, "dyn", []);
    public static readonly CheckedType Error = new(CheckedKind.Error, "error", []);
    public static readonly CheckedType Int = new(CheckedKind.Int, "int", []);
    public static readonly CheckedType Null = new(CheckedKind.Null, "null_type", []);
    public static readonly CheckedType String = new(CheckedKind.String, "string", []);
    public static readonly CheckedType Timestamp = new(CheckedKind.Timestamp, "google.protobuf.Timestamp", []);
    public static readonly CheckedType TypeType = new(CheckedKind.Type, "type", []);
    public static readonly CheckedType Uint = new(CheckedKind.Uint, "uint", []);

    /// <summary>The kind of the type.</summary>
    public CheckedKind Kind { get; }

    /// <summary>The type-erased runtime type name (cel-go's <c>TypeName()</c>).</summary>
    public string Name { get; }

    /// <summary>The type parameters, if any.</summary>
    public IReadOnlyList<CheckedType> Parameters { get; }

    /// <summary>A list of the given element type.</summary>
    public static CheckedType List(CheckedType element) => new(CheckedKind.List, "list", [element]);

    /// <summary>A map of the given key and value types.</summary>
    public static CheckedType Map(CheckedType key, CheckedType value) => new(CheckedKind.Map, "map", [key, value]);

    /// <summary>An abstract parameterized type with the given name.</summary>
    public static CheckedType Opaque(string name, params CheckedType[] parameters) => new(CheckedKind.Opaque, name, parameters);

    /// <summary>CEL's <c>optional_type(T)</c>.</summary>
    public static CheckedType Optional(CheckedType parameter) => Opaque("optional_type", parameter);

    /// <summary>A type parameter, bound (or not) during checking.</summary>
    public static CheckedType TypeParam(string name) => new(CheckedKind.TypeParam, name, []);

    /// <summary>The type of a type value, <c>type(T)</c>.</summary>
    public static CheckedType TypeOf(CheckedType parameter) => new(CheckedKind.Type, "type", [parameter]);

    /// <summary>
    /// A reference to an externally defined (message) type. As in cel-go's <c>NewObjectType</c>, the
    /// protobuf well-known types are sanitized to their CEL equivalents.
    /// </summary>
    public static CheckedType Object(string typeName) =>
        typeName switch
        {
            "google.protobuf.BoolValue" => Nullable(Bool),
            "google.protobuf.BytesValue" => Nullable(Bytes),
            "google.protobuf.DoubleValue" => Nullable(Double),
            "google.protobuf.FloatValue" => Nullable(Double),
            "google.protobuf.Int64Value" => Nullable(Int),
            "google.protobuf.Int32Value" => Nullable(Int),
            "google.protobuf.UInt64Value" => Nullable(Uint),
            "google.protobuf.UInt32Value" => Nullable(Uint),
            "google.protobuf.StringValue" => Nullable(String),
            "google.protobuf.Any" => Any,
            "google.protobuf.Duration" => Duration,
            "google.protobuf.Timestamp" => Timestamp,
            "google.protobuf.ListValue" => List(Dyn),
            "google.protobuf.NullValue" => Null,
            "google.protobuf.Struct" => Map(String, Dyn),
            "google.protobuf.Value" => Dyn,
            _ => new CheckedType(CheckedKind.Struct, typeName, []),
        };

    /// <summary>A nullable wrapper of a primitive type (cel-go's <c>NewNullableType</c>).</summary>
    private static CheckedType Nullable(CheckedType wrapped) =>
        new(wrapped.Kind, wrapped.Name, wrapped.Parameters, wrapped);

    /// <summary>True for dyn, any and type parameters: cel-go's private <c>Type.isDyn</c>.</summary>
    public bool IsDynamic => Kind is CheckedKind.Dyn or CheckedKind.Any or CheckedKind.TypeParam;

    /// <summary>Whether the two types are exactly the same, type parameter names included.</summary>
    public bool IsExactType(CheckedType other)
    {
        if (ReferenceEquals(this, other))
            return true;
        if (Kind != other.Kind || Parameters.Count != other.Parameters.Count || Name != other.Name)
            return false;
        for (var i = 0; i < Parameters.Count; i++)
        {
            if (!Parameters[i].IsExactType(other.Parameters[i]))
                return false;
        }
        return true;
    }

    /// <summary>Whether this type is type-check assignable from <paramref name="from"/>.</summary>
    public bool IsAssignableType(CheckedType from)
    {
        if (_nullableOf is not null)
            return Null.IsAssignableType(from) || _nullableOf.IsAssignableType(from);

        if (ReferenceEquals(this, from) || IsDynamic)
            return true;
        if (Kind != from.Kind || Name != from.Name || Parameters.Count != from.Parameters.Count)
            return false;
        for (var i = 0; i < Parameters.Count; i++)
        {
            if (!Parameters[i].IsAssignableType(from.Parameters[i]))
                return false;
        }
        return true;
    }

    /// <summary>The parameterized type-check name: <c>wrapper(T)</c> for a nullable wrapper.</summary>
    public string DeclaredTypeName =>
        Kind != CheckedKind.Null && !IsDynamic && IsAssignableType(Null) ? $"wrapper({Name})" : Name;

    /// <summary>cel-go's <c>Type.String()</c>, used for the non-boolean caveat output message.</summary>
    public override string ToString() =>
        Parameters.Count == 0
            ? DeclaredTypeName
            : $"{DeclaredTypeName}({string.Join(", ", Parameters.Select(p => p.ToString()))})";

    /// <summary>cel-go's <c>checker.FormatCELType</c>, used in type-check error messages.</summary>
    public string Format()
    {
        switch (Kind)
        {
            case CheckedKind.Any:
                return "any";
            case CheckedKind.Duration:
                return "duration";
            case CheckedKind.Error:
                return "!error!";
            case CheckedKind.Null:
                return "null";
            case CheckedKind.Timestamp:
                return "timestamp";
            case CheckedKind.TypeParam:
                return Name;
            case CheckedKind.Opaque when Name == "function":
                return FormatFunction(Parameters[0], Parameters.Skip(1).ToList(), isInstance: false);
            case CheckedKind.Unspecified:
                return string.Empty;
        }

        return Parameters.Count == 0
            ? DeclaredTypeName
            : $"{Name}({string.Join(", ", Parameters.Select(p => p.Format()))})";
    }

    /// <summary>cel-go's <c>formatFunctionDeclType</c>.</summary>
    public static string FormatFunction(CheckedType? result, IReadOnlyList<CheckedType> args, bool isInstance)
    {
        var text = new System.Text.StringBuilder();
        var rest = args;
        if (isInstance)
        {
            text.Append(args[0].Format()).Append('.');
            rest = args.Skip(1).ToList();
        }

        text.Append('(').Append(string.Join(", ", rest.Select(a => a.Format()))).Append(')');
        var formattedResult = result?.Format() ?? string.Empty;
        if (formattedResult.Length > 0)
            text.Append(" -> ").Append(formattedResult);
        return text.ToString();
    }
}
