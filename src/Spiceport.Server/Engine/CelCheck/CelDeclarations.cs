namespace Spiceport.Engine;

/// <summary>
/// One overload of a declared CEL function: cel-go's <c>decls.OverloadDecl</c>, reduced to what the type
/// checker reads.
/// </summary>
/// <param name="Id">The overload id.</param>
/// <param name="IsMember">True for a receiver-style overload (the receiver is the first argument type).</param>
/// <param name="ArgTypes">The argument types, receiver first for a member overload.</param>
/// <param name="ResultType">The result type.</param>
/// <param name="CrossTypeNumericComparison">
/// True for the mixed int/uint/double ordering overloads, which cel-go's checker disables unless the
/// environment enables cross-type numeric comparisons (SpiceDB's does not).
/// </param>
internal sealed record CelOverload(
    string Id,
    bool IsMember,
    IReadOnlyList<CheckedType> ArgTypes,
    CheckedType ResultType,
    bool CrossTypeNumericComparison = false)
{
    /// <summary>The names of the type parameters occurring in the signature, as <c>decls.Overload</c> collects them.</summary>
    public IReadOnlyList<string> TypeParams { get; } = CollectTypeParams([.. ArgTypes, ResultType]);

    private static List<string> CollectTypeParams(IEnumerable<CheckedType> types)
    {
        var names = new List<string>();
        void Collect(CheckedType t)
        {
            if (t.Kind == CheckedKind.TypeParam && !names.Contains(t.Name))
                names.Add(t.Name);
            foreach (var p in t.Parameters)
                Collect(p);
        }

        foreach (var t in types)
            Collect(t);
        return names;
    }
}

/// <summary>A declared CEL function and its overloads, in declaration order.</summary>
internal sealed record CelFunction(string Name, IReadOnlyList<CelOverload> Overloads);

/// <summary>
/// The static declarations of the CEL environment SpiceDB compiles caveats in
/// (<c>pkg/caveats/env.go</c> <c>asCelEnvironment</c>): <c>cel.NewEnv</c>'s standard library
/// (<c>common/stdlib/standard.go</c>), the optional-types library at version 0
/// (<c>cel/library.go</c>, <c>cel.OptionalTypes(cel.OptionalTypesVersion(0))</c>), and the caveat type
/// set's custom functions (<c>pkg/caveats/types/basic.go</c>, <c>ipaddress.go</c>): <c>isSubtreeOf</c> on
/// <c>map(string, dyn)</c> and <c>in_cidr</c> on <c>IPAddress</c>. Mirrors <c>authzed/cel-go</c> v0.20.2,
/// the fork SpiceDB pins.
/// </summary>
/// <remarks>
/// There is deliberately no <c>ipaddress(string)</c> function: SpiceDB declares <c>ipaddress</c> only as a
/// parameter type, so a caveat calling it fails SpiceDB's checker, even though the runtime
/// <see cref="CaveatCelEnvironment"/> binds one for evaluation.
/// </remarks>
internal static class CelDeclarations
{
    private static readonly CheckedType A = CheckedType.TypeParam("A");
    private static readonly CheckedType B = CheckedType.TypeParam("B");
    private static readonly CheckedType K = CheckedType.TypeParam("K");
    private static readonly CheckedType V = CheckedType.TypeParam("V");

    /// <summary>cel-go's <c>cel.ObjectType("IPAddress")</c>, the CEL type of an <c>ipaddress</c> parameter.</summary>
    public static readonly CheckedType IPAddress = CheckedType.Object("IPAddress");

    /// <summary>The functions, by name.</summary>
    public static readonly IReadOnlyDictionary<string, CelFunction> Functions = BuildFunctions();

    /// <summary>
    /// The standard type identifiers (<c>stdlib.Types()</c>: <c>decls.TypeVariable</c> of each standard
    /// type, named by its runtime type name).
    /// </summary>
    public static readonly IReadOnlyDictionary<string, CheckedType> TypeIdentifiers =
        new[]
        {
            CheckedType.Bool, CheckedType.Bytes, CheckedType.Double, CheckedType.Duration, CheckedType.Int,
            CheckedType.List(A), CheckedType.Map(A, B), CheckedType.Null, CheckedType.String,
            CheckedType.Timestamp, CheckedType.TypeType, CheckedType.Uint,
        }.ToDictionary(t => t.Name, CheckedType.TypeOf);

    /// <summary>
    /// The registry types found by <c>Provider.FindIdent</c> that the declarations above do not already
    /// shadow: the optional type registered by the optional library.
    /// </summary>
    public static readonly IReadOnlyDictionary<string, CheckedType> RegistryTypeIdentifiers =
        new Dictionary<string, CheckedType>
        {
            ["optional_type"] = CheckedType.TypeOf(CheckedType.Opaque("optional_type")),
        };

    /// <summary>The enum values the default registry resolves as identifiers.</summary>
    public static readonly IReadOnlySet<string> EnumValueIdentifiers =
        new HashSet<string> { "google.protobuf.NullValue.NULL_VALUE" };

    /// <summary>
    /// The message types of the default registry (the protobuf well-known types cel-go registers) and
    /// their fields' CEL types, as <c>Provider.FindStructType</c> / <c>FindStructFieldType</c> report them.
    /// </summary>
    public static readonly IReadOnlyDictionary<string, IReadOnlyDictionary<string, CheckedType>> StructTypes =
        new Dictionary<string, IReadOnlyDictionary<string, CheckedType>>
        {
            ["google.protobuf.Any"] = Fields(("type_url", CheckedType.String), ("value", CheckedType.Bytes)),
            ["google.protobuf.Duration"] = Fields(("seconds", CheckedType.Int), ("nanos", CheckedType.Int)),
            ["google.protobuf.Timestamp"] = Fields(("seconds", CheckedType.Int), ("nanos", CheckedType.Int)),
            ["google.protobuf.Empty"] = Fields(),
            ["google.protobuf.FieldMask"] = Fields(("paths", CheckedType.List(CheckedType.String))),
            ["google.protobuf.Struct"] = Fields(("fields", CheckedType.Map(CheckedType.String, CheckedType.Dyn))),
            ["google.protobuf.Value"] = Fields(
                ("null_value", CheckedType.Null),
                ("number_value", CheckedType.Double),
                ("string_value", CheckedType.String),
                ("bool_value", CheckedType.Bool),
                ("struct_value", CheckedType.Map(CheckedType.String, CheckedType.Dyn)),
                ("list_value", CheckedType.List(CheckedType.Dyn))),
            ["google.protobuf.ListValue"] = Fields(("values", CheckedType.List(CheckedType.Dyn))),
            ["google.protobuf.BoolValue"] = Fields(("value", CheckedType.Bool)),
            ["google.protobuf.BytesValue"] = Fields(("value", CheckedType.Bytes)),
            ["google.protobuf.DoubleValue"] = Fields(("value", CheckedType.Double)),
            ["google.protobuf.FloatValue"] = Fields(("value", CheckedType.Double)),
            ["google.protobuf.Int64Value"] = Fields(("value", CheckedType.Int)),
            ["google.protobuf.Int32Value"] = Fields(("value", CheckedType.Int)),
            ["google.protobuf.UInt64Value"] = Fields(("value", CheckedType.Uint)),
            ["google.protobuf.UInt32Value"] = Fields(("value", CheckedType.Uint)),
            ["google.protobuf.StringValue"] = Fields(("value", CheckedType.String)),
        };

    private static IReadOnlyDictionary<string, CheckedType> Fields(params (string Name, CheckedType Type)[] fields) =>
        fields.ToDictionary(f => f.Name, f => f.Type);

    private static Dictionary<string, CelFunction> BuildFunctions()
    {
        var bool_ = CheckedType.Bool;
        var bytes = CheckedType.Bytes;
        var dbl = CheckedType.Double;
        var dur = CheckedType.Duration;
        var int_ = CheckedType.Int;
        var str = CheckedType.String;
        var ts = CheckedType.Timestamp;
        var uint_ = CheckedType.Uint;
        var listOfA = CheckedType.List(A);
        var mapOfAB = CheckedType.Map(A, B);
        var optV = CheckedType.Optional(V);
        var listOfV = CheckedType.List(V);
        var mapOfKV = CheckedType.Map(K, V);

        static CelOverload G(string id, CheckedType result, params CheckedType[] args) => new(id, false, args, result);
        static CelOverload M(string id, CheckedType result, params CheckedType[] args) => new(id, true, args, result);
        static CelOverload X(string id, CheckedType result, params CheckedType[] args) =>
            new(id, false, args, result, CrossTypeNumericComparison: true);

        CelOverload[] Ordering(string name) =>
        [
            G($"{name}_bool", bool_, bool_, bool_),
            G($"{name}_int64", bool_, int_, int_),
            X($"{name}_int64_double", bool_, int_, dbl),
            X($"{name}_int64_uint64", bool_, int_, uint_),
            G($"{name}_uint64", bool_, uint_, uint_),
            X($"{name}_uint64_double", bool_, uint_, dbl),
            X($"{name}_uint64_int64", bool_, uint_, int_),
            G($"{name}_double", bool_, dbl, dbl),
            X($"{name}_double_int64", bool_, dbl, int_),
            X($"{name}_double_uint64", bool_, dbl, uint_),
            G($"{name}_string", bool_, str, str),
            G($"{name}_bytes", bool_, bytes, bytes),
            G($"{name}_timestamp", bool_, ts, ts),
            G($"{name}_duration", bool_, dur, dur),
        ];

        CelOverload[] TimeAccessor(string id, bool withDuration) =>
            withDuration
                ? [M($"timestamp_to_{id}", int_, ts), M($"timestamp_to_{id}_with_tz", int_, ts, str), M($"duration_to_{id}", int_, dur)]
                : [M($"timestamp_to_{id}", int_, ts), M($"timestamp_to_{id}_with_tz", int_, ts, str)];

        var functions = new List<CelFunction>
        {
            // Standard library (common/stdlib/standard.go).
            new("_?_:_", [G("conditional", A, bool_, A, A)]),
            new("_&&_", [G("logical_and", bool_, bool_, bool_)]),
            new("_||_", [G("logical_or", bool_, bool_, bool_)]),
            new("!_", [G("logical_not", bool_, bool_)]),
            new("@not_strictly_false", [G("not_strictly_false", bool_, bool_)]),
            new("_==_", [G("equals", bool_, A, A)]),
            new("_!=_", [G("not_equals", bool_, A, A)]),
            new("_+_",
            [
                G("add_bytes", bytes, bytes, bytes),
                G("add_double", dbl, dbl, dbl),
                G("add_duration_duration", dur, dur, dur),
                G("add_duration_timestamp", ts, dur, ts),
                G("add_timestamp_duration", ts, ts, dur),
                G("add_int64", int_, int_, int_),
                G("add_list", listOfA, listOfA, listOfA),
                G("add_string", str, str, str),
                G("add_uint64", uint_, uint_, uint_),
            ]),
            new("_/_", [G("divide_double", dbl, dbl, dbl), G("divide_int64", int_, int_, int_), G("divide_uint64", uint_, uint_, uint_)]),
            new("_%_", [G("modulo_int64", int_, int_, int_), G("modulo_uint64", uint_, uint_, uint_)]),
            new("_*_", [G("multiply_double", dbl, dbl, dbl), G("multiply_int64", int_, int_, int_), G("multiply_uint64", uint_, uint_, uint_)]),
            new("-_", [G("negate_double", dbl, dbl), G("negate_int64", int_, int_)]),
            new("_-_",
            [
                G("subtract_double", dbl, dbl, dbl),
                G("subtract_duration_duration", dur, dur, dur),
                G("subtract_int64", int_, int_, int_),
                G("subtract_timestamp_duration", ts, ts, dur),
                G("subtract_timestamp_timestamp", dur, ts, ts),
                G("subtract_uint64", uint_, uint_, uint_),
            ]),
            new("_<_", Ordering("less")),
            new("_<=_", Ordering("less_equals")),
            new("_>_", Ordering("greater")),
            new("_>=_", Ordering("greater_equals")),
            new("_[_]", [G("index_list", A, listOfA, int_), G("index_map", B, mapOfAB, A)]),
            new("@in", [G("in_list", bool_, A, listOfA), G("in_map", bool_, A, mapOfAB)]),
            new("size",
            [
                G("size_bytes", int_, bytes), M("bytes_size", int_, bytes),
                G("size_list", int_, listOfA), M("list_size", int_, listOfA),
                G("size_map", int_, mapOfAB), M("map_size", int_, mapOfAB),
                G("size_string", int_, str), M("string_size", int_, str),
            ]),
            new("type", [G("type", CheckedType.TypeOf(A), A)]),
            new("bool", [G("bool_to_bool", bool_, bool_), G("string_to_bool", bool_, str)]),
            new("bytes", [G("bytes_to_bytes", bytes, bytes), G("string_to_bytes", bytes, str)]),
            new("double",
            [
                G("double_to_double", dbl, dbl), G("int64_to_double", dbl, int_),
                G("string_to_double", dbl, str), G("uint64_to_double", dbl, uint_),
            ]),
            new("duration", [G("duration_to_duration", dur, dur), G("int64_to_duration", dur, int_), G("string_to_duration", dur, str)]),
            new("dyn", [G("to_dyn", CheckedType.Dyn, A)]),
            new("int",
            [
                G("int64_to_int64", int_, int_), G("double_to_int64", int_, dbl), G("duration_to_int64", int_, dur),
                G("string_to_int64", int_, str), G("timestamp_to_int64", int_, ts), G("uint64_to_int64", int_, uint_),
            ]),
            new("string",
            [
                G("string_to_string", str, str), G("bool_to_string", str, bool_), G("bytes_to_string", str, bytes),
                G("double_to_string", str, dbl), G("duration_to_string", str, dur), G("int64_to_string", str, int_),
                G("timestamp_to_string", str, ts), G("uint64_to_string", str, uint_),
            ]),
            new("timestamp", [G("timestamp_to_timestamp", ts, ts), G("int64_to_timestamp", ts, int_), G("string_to_timestamp", ts, str)]),
            new("uint",
            [
                G("uint64_to_uint64", uint_, uint_), G("double_to_uint64", uint_, dbl),
                G("int64_to_uint64", uint_, int_), G("string_to_uint64", uint_, str),
            ]),
            new("contains", [M("contains_string", bool_, str, str)]),
            new("endsWith", [M("ends_with_string", bool_, str, str)]),
            new("startsWith", [M("starts_with_string", bool_, str, str)]),
            new("matches", [G("matches", bool_, str, str), M("matches_string", bool_, str, str)]),
            new("getFullYear", TimeAccessor("year", withDuration: false)),
            new("getMonth", TimeAccessor("month", withDuration: false)),
            new("getDayOfYear", TimeAccessor("day_of_year", withDuration: false)),
            new("getDayOfMonth", TimeAccessor("day_of_month", withDuration: false)),
            new("getDate", TimeAccessor("day_of_month_1_based", withDuration: false)),
            new("getDayOfWeek", TimeAccessor("day_of_week", withDuration: false)),
            new("getHours", TimeAccessor("hours", withDuration: true)),
            new("getMinutes", TimeAccessor("minutes", withDuration: true)),
            new("getSeconds", TimeAccessor("seconds", withDuration: true)),
            new("getMilliseconds", TimeAccessor("milliseconds", withDuration: true)),

            // Optional types library, version 0 (cel/library.go).
            new("optional.of", [G("optional_of", optV, V)]),
            new("optional.ofNonZeroValue", [G("optional_ofNonZeroValue", optV, V)]),
            new("optional.none", [G("optional_none", optV)]),
            new("value", [M("optional_value", V, optV)]),
            new("hasValue", [M("optional_hasValue", bool_, optV)]),
            new("or", [M("optional_or_optional", optV, optV, optV)]),
            new("orValue", [M("optional_orValue_value", V, optV, V)]),
            new("_?._", [G("select_optional_field", optV, CheckedType.Dyn, str)]),
            new("_[?_]",
            [
                G("list_optindex_optional_int", optV, listOfV, int_),
                G("optional_list_optindex_optional_int", optV, CheckedType.Optional(listOfV), int_),
                G("map_optindex_optional_value", optV, mapOfKV, K),
                G("optional_map_optindex_optional_value", optV, CheckedType.Optional(mapOfKV), K),
            ]),

            // The caveat type set's custom functions (pkg/caveats/types).
            new("isSubtreeOf",
            [
                M("isSubtreeOf", bool_, CheckedType.Map(str, CheckedType.Dyn), CheckedType.Map(str, CheckedType.Dyn)),
            ]),
            new("in_cidr", [M("ipaddress_in_cidr_string", bool_, IPAddress, str)]),
        };

        // The optional library adds overloads to the standard index operator; cel-go merges them in
        // declaration order after the standard ones.
        var byName = functions.ToDictionary(f => f.Name);
        byName["_[_]"] = byName["_[_]"] with
        {
            Overloads =
            [
                .. byName["_[_]"].Overloads,
                G("optional_list_index_int", optV, CheckedType.Optional(listOfV), int_),
                G("optional_map_index_value", optV, CheckedType.Optional(mapOfKV), K),
            ],
        };
        return byName;
    }
}
