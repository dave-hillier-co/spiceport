namespace Spiceport.Engine;

/// <summary>
/// A static CEL type checker over a parsed expression (<see cref="CelExpr"/>): a transliteration of
/// cel-go's <c>checker/checker.go</c>, <c>checker/types.go</c> and the parts of <c>checker/env.go</c> the
/// SpiceDB caveat environment exercises (<c>authzed/cel-go</c> v0.20.2). It computes each node's type,
/// unifying the standard library's type-parameterized overloads, and reports the errors cel-go's checker
/// reports, with its wording.
/// </summary>
/// <remarks>
/// Faithfully reproduced cel-go behaviours that are easy to "fix" by accident:
/// <list type="bullet">
///   <item>Mixed int/uint/double ordering overloads (<c>1 &lt; 1.0</c>) are disabled, but a comprehension
///   (any macro) replaces the environment with one built without that filter, so they are enabled from
///   then on (cel-go's <c>Env.enterScope</c>/<c>exitScope</c> drop <c>filteredOverloadIDs</c>).</item>
///   <item>Heterogeneous list and map literals join to <c>dyn</c> rather than failing.</item>
///   <item>A <c>dyn</c> or error operand is assignable everywhere, so a type error is reported once, at
///   its source, and does not cascade.</item>
/// </list>
/// </remarks>
internal sealed class CelTypeChecker
{
    private readonly CelIssues _errors;
    private readonly Dictionary<CelExpr, CheckedType> _types = new(ReferenceEqualityComparer.Instance);
    private readonly List<Dictionary<string, CheckedType>> _scopes;
    private Dictionary<string, CheckedType> _mapping = [];
    private int _freeTypeVarCounter;
    private bool _crossTypeNumericComparisonsDisabled = true;

    private CelTypeChecker(IReadOnlyDictionary<string, CheckedType> variables, CelIssues errors)
    {
        _errors = errors;
        var global = new Dictionary<string, CheckedType>(CelDeclarations.TypeIdentifiers);
        foreach (var (name, type) in variables)
            global[name] = type;
        _scopes = [global];
    }

    /// <summary>
    /// Checks <paramref name="expr"/> against the declared <paramref name="variables"/>, reporting type
    /// errors to <paramref name="errors"/>, and returns the expression's output type with unbound type
    /// parameters replaced by <c>dyn</c> (cel-go's <c>ast.OutputType()</c> after <c>checker.Check</c>).
    /// </summary>
    public static CheckedType Check(CelExpr expr, IReadOnlyDictionary<string, CheckedType> variables, CelIssues errors)
    {
        var checker = new CelTypeChecker(variables, errors);
        checker.CheckExpr(expr);
        return Substitute(checker._mapping, checker.GetType(expr) ?? CheckedType.Dyn, typeParamToDyn: true);
    }

    private void CheckExpr(CelExpr? e)
    {
        switch (e)
        {
            case null:
                return;
            case CelLiteral literal:
                SetType(e, literal.Kind switch
                {
                    CelLiteralKind.Bool => CheckedType.Bool,
                    CelLiteralKind.Bytes => CheckedType.Bytes,
                    CelLiteralKind.Double => CheckedType.Double,
                    CelLiteralKind.Int => CheckedType.Int,
                    CelLiteralKind.Null => CheckedType.Null,
                    CelLiteralKind.String => CheckedType.String,
                    CelLiteralKind.Uint => CheckedType.Uint,
                    _ => throw new InvalidOperationException($"unexpected literal kind {literal.Kind}"),
                });
                return;
            case CelIdent ident:
                CheckIdent(ident);
                return;
            case CelSelect select:
                CheckSelect(select);
                return;
            case CelCall call:
                CheckCall(call);
                return;
            case CelList list:
                CheckCreateList(list);
                return;
            case CelMap map:
                CheckCreateMap(map);
                return;
            case CelStruct str:
                CheckCreateStruct(str);
                return;
            case CelComprehension comprehension:
                CheckComprehension(comprehension);
                return;
            default:
                _errors.Report(e.Location, "unexpected unspecified type: Expr");
                return;
        }
    }

    private void CheckIdent(CelIdent e)
    {
        if (LookupIdent(e.Name) is { } type)
        {
            SetType(e, type);
            return;
        }

        SetType(e, CheckedType.Error);
        UndeclaredReference(e.Location, e.Name);
    }

    private void CheckSelect(CelSelect e)
    {
        // Before traversing down the tree, try to interpret as a qualified name.
        if (ToQualifiedName(e) is { } qualified && LookupIdent(qualified) is { } identType)
        {
            SetType(e, identType);
            return;
        }

        var resultType = CheckSelectField(e, e.Operand, e.Field, optional: false);
        if (e.TestOnly)
            resultType = CheckedType.Bool;
        SetType(e, Substitute(_mapping, resultType, typeParamToDyn: false));
    }

    private void CheckOptSelect(CelCall e)
    {
        var operand = e.Args[0];
        var field = e.Args[1];
        if (field is not CelLiteral { Kind: CelLiteralKind.String, StringValue: { } fieldName })
        {
            _errors.Report(field.Location, "unsupported optional field selection");
            return;
        }

        var resultType = CheckSelectField(e, operand, fieldName, optional: true);
        SetType(e, Substitute(_mapping, resultType, typeParamToDyn: false));
    }

    private CheckedType CheckSelectField(CelExpr e, CelExpr operand, string field, bool optional)
    {
        CheckExpr(operand);
        var operandType = Substitute(_mapping, GetType(operand)!, typeParamToDyn: false);
        var (targetType, isOpt) = MaybeUnwrapOptional(operandType);

        var resultType = CheckedType.Error;
        switch (targetType.Kind)
        {
            case CheckedKind.Map:
                resultType = targetType.Parameters[1];
                break;
            case CheckedKind.Struct:
                if (LookupFieldType(e.Location, targetType.Name, field) is { } fieldType)
                    resultType = fieldType;
                break;
            case CheckedKind.TypeParam:
                IsAssignable(CheckedType.Dyn, targetType);
                resultType = CheckedType.Dyn;
                break;
            default:
                if (!IsDynOrError(targetType))
                    _errors.Report(e.Location, $"type '{targetType.Format()}' does not support field selection");
                resultType = CheckedType.Dyn;
                break;
        }

        return isOpt || optional ? CheckedType.Optional(resultType) : resultType;
    }

    private void CheckCall(CelCall e)
    {
        if (e.Function == "_?._")
        {
            CheckOptSelect(e);
            return;
        }

        foreach (var arg in e.Args)
            CheckExpr(arg);

        if (e.Target is null)
        {
            var fn = LookupFunction(e.Function);
            if (fn is null)
            {
                UndeclaredReference(e.Location, e.Function);
                SetType(e, CheckedType.Error);
                return;
            }

            ResolveOverloadOrError(e, fn, target: null, e.Args);
            return;
        }

        // A receiver may be a namespace: given a.b.c(), either a.b.c is a function or c is a function
        // with target a.b.
        if (ToQualifiedName(e.Target) is { } qualifiedPrefix
            && LookupFunction(qualifiedPrefix + "." + e.Function) is { } namespaced)
        {
            ResolveOverloadOrError(e, namespaced, target: null, e.Args);
            return;
        }

        CheckExpr(e.Target);
        var member = LookupFunction(e.Function);
        if (member is not null)
        {
            ResolveOverloadOrError(e, member, e.Target, e.Args);
            return;
        }

        SetType(e, CheckedType.Error);
        UndeclaredReference(e.Location, e.Function);
    }

    private void ResolveOverloadOrError(CelExpr e, CelFunction fn, CelExpr? target, IReadOnlyList<CelExpr> args)
    {
        var resolution = ResolveOverload(e, fn, target, args);
        SetType(e, resolution ?? CheckedType.Error);
    }

    private CheckedType? ResolveOverload(CelExpr call, CelFunction fn, CelExpr? target, IReadOnlyList<CelExpr> args)
    {
        var argTypes = new List<CheckedType>();
        if (target is not null)
            argTypes.Add(GetType(target)!);
        argTypes.AddRange(args.Select(a => GetType(a)!));

        CheckedType? resultType = null;
        foreach (var overload in fn.Overloads)
        {
            if (_crossTypeNumericComparisonsDisabled && overload.CrossTypeNumericComparison)
                continue;
            if ((target is null && overload.IsMember) || (target is not null && !overload.IsMember))
                continue;

            // Logical operators are checked argument by argument, so a non-bool operand is reported
            // at the operand rather than as an overload failure.
            if (fn.Name is "_&&_" or "_||_")
            {
                for (var i = 0; i < argTypes.Count; i++)
                {
                    if (!IsAssignable(argTypes[i], CheckedType.Bool))
                    {
                        TypeMismatch(args[i].Location, CheckedType.Bool, argTypes[i]);
                        resultType = CheckedType.Error;
                    }
                }

                return resultType is not null && IsError(resultType) ? null : CheckedType.Bool;
            }

            var overloadType = CheckedType.Opaque("function", [overload.ResultType, .. overload.ArgTypes]);
            if (overload.TypeParams.Count != 0)
            {
                var substitutions = new Dictionary<string, CheckedType>();
                foreach (var typeParam in overload.TypeParams)
                    substitutions[typeParam] = NewTypeVar();
                overloadType = Substitute(substitutions, overloadType, typeParamToDyn: false);
            }

            var candidateArgTypes = overloadType.Parameters.Skip(1).ToList();
            if (IsAssignableList(argTypes, candidateArgTypes))
            {
                var fnResultType = Substitute(_mapping, overloadType.Parameters[0], typeParamToDyn: false);
                if (resultType is null)
                    resultType = fnResultType;
                else if (!IsDyn(resultType) && !fnResultType.IsExactType(resultType))
                    resultType = CheckedType.Dyn;
            }
        }

        if (resultType is null)
        {
            var formatted = argTypes.Select(t => Substitute(_mapping, t, typeParamToDyn: true)).ToList();
            var signature = CheckedType.FormatFunction(null, formatted, target is not null);
            _errors.Report(call.Location, $"found no matching overload for '{fn.Name}' applied to '{signature}'");
            return null;
        }

        return resultType;
    }

    private void CheckCreateList(CelList e)
    {
        CheckedType? elemsType = null;
        for (var i = 0; i < e.Elements.Count; i++)
        {
            var element = e.Elements[i];
            CheckExpr(element);
            var elemType = GetType(element)!;
            if (e.OptionalIndices.Contains(i))
            {
                (elemType, var isOptional) = MaybeUnwrapOptional(elemType);
                if (!isOptional && !IsDyn(elemType))
                    TypeMismatch(element.Location, CheckedType.Optional(elemType), elemType);
            }

            elemsType = JoinTypes(element, elemsType, elemType);
        }

        SetType(e, CheckedType.List(elemsType ?? NewTypeVar()));
    }

    private void CheckCreateMap(CelMap e)
    {
        CheckedType? keyType = null;
        CheckedType? valueType = null;
        foreach (var entry in e.Entries)
        {
            CheckExpr(entry.Key);
            keyType = JoinTypes(entry.Key, keyType, GetType(entry.Key)!);

            CheckExpr(entry.Value);
            var valType = GetType(entry.Value)!;
            if (entry.Optional)
            {
                (valType, var isOptional) = MaybeUnwrapOptional(valType);
                if (!isOptional && !IsDyn(valType))
                    TypeMismatch(entry.Value.Location, CheckedType.Optional(valType), valType);
            }

            valueType = JoinTypes(entry.Value, valueType, valType);
        }

        if (keyType is null)
        {
            keyType = NewTypeVar();
            valueType = NewTypeVar();
        }

        SetType(e, CheckedType.Map(keyType, valueType!));
    }

    private void CheckCreateStruct(CelStruct e)
    {
        var resultType = CheckedType.Error;
        var identType = LookupIdent(e.TypeName);
        if (identType is null)
        {
            UndeclaredReference(e.Location, e.TypeName);
            SetType(e, CheckedType.Error);
            return;
        }

        // The identifier's fully qualified name: a leading dot only roots the lookup.
        var typeName = e.TypeName.StartsWith('.') ? e.TypeName[1..] : e.TypeName;
        if (identType.Kind != CheckedKind.Error)
        {
            if (identType.Kind != CheckedKind.Type)
            {
                _errors.Report(e.Location, $"'{identType.DeclaredTypeName}' is not a type");
            }
            else
            {
                resultType = identType.Parameters[0];
                if (IsWellKnownType(resultType))
                {
                    typeName = WellKnownTypeName(resultType);
                }
                else if (resultType.Kind == CheckedKind.Struct)
                {
                    typeName = resultType.DeclaredTypeName;
                }
                else
                {
                    _errors.Report(e.Location, $"'{resultType.DeclaredTypeName}' is not a message type");
                    resultType = CheckedType.Error;
                }
            }
        }

        SetType(e, resultType);

        foreach (var field in e.Fields)
        {
            CheckExpr(field.Value);
            var fieldType = LookupFieldType(field.Location, typeName, field.Name) ?? CheckedType.Error;
            var valType = GetType(field.Value)!;
            if (field.Optional)
            {
                (valType, var isOptional) = MaybeUnwrapOptional(valType);
                if (!isOptional && !IsDyn(valType))
                    TypeMismatch(field.Value.Location, CheckedType.Optional(valType), valType);
            }

            if (!IsAssignable(fieldType, valType))
            {
                _errors.Report(field.Location,
                    $"expected type of field '{field.Name}' is '{fieldType.Format()}' but provided type is '{valType.Format()}'");
            }
        }
    }

    private void CheckComprehension(CelComprehension e)
    {
        CheckExpr(e.IterRange);
        CheckExpr(e.AccuInit);
        var accuType = GetType(e.AccuInit)!;
        var rangeType = Substitute(_mapping, GetType(e.IterRange)!, typeParamToDyn: false);
        CheckedType varType;
        switch (rangeType.Kind)
        {
            case CheckedKind.List:
            case CheckedKind.Map:
                // A map ranges over its keys.
                varType = rangeType.Parameters[0];
                break;
            case CheckedKind.Dyn:
            case CheckedKind.Error:
            case CheckedKind.TypeParam:
                IsAssignable(CheckedType.Dyn, rangeType);
                varType = CheckedType.Dyn;
                break;
            default:
                _errors.Report(e.IterRange.Location,
                    $"expression of type '{rangeType.Format()}' cannot be range of a comprehension (must be list, map, or dynamic)");
                varType = CheckedType.Error;
                break;
        }

        // cel-go builds each scoped environment without the cross-type numeric comparison filter, and the
        // environment it restores on exit is rebuilt the same way, so the filter is gone for good.
        _crossTypeNumericComparisonsDisabled = false;

        _scopes.Add(new Dictionary<string, CheckedType> { [e.AccuVar] = accuType });
        _scopes.Add(new Dictionary<string, CheckedType> { [e.IterVar] = varType });
        CheckExpr(e.LoopCondition);
        AssertType(e.LoopCondition, CheckedType.Bool);
        CheckExpr(e.LoopStep);
        AssertType(e.LoopStep, accuType);
        _scopes.RemoveAt(_scopes.Count - 1);
        CheckExpr(e.Result);
        _scopes.RemoveAt(_scopes.Count - 1);
        SetType(e, Substitute(_mapping, GetType(e.Result)!, typeParamToDyn: false));
    }

    private CheckedType JoinTypes(CelExpr e, CheckedType? previous, CheckedType current)
    {
        if (previous is null)
            return current;
        if (IsAssignable(previous, current))
            return MostGeneral(previous, current);

        // SpiceDB's environment keeps cel-go's default of dynamic aggregate literal element types.
        return CheckedType.Dyn;
    }

    private CheckedType NewTypeVar() => CheckedType.TypeParam($"_var{_freeTypeVarCounter++}");

    private bool IsAssignable(CheckedType t1, CheckedType t2)
    {
        var copy = new Dictionary<string, CheckedType>(_mapping);
        if (!InternalIsAssignable(copy, t1, t2))
            return false;
        _mapping = copy;
        return true;
    }

    private bool IsAssignableList(IReadOnlyList<CheckedType> l1, IReadOnlyList<CheckedType> l2)
    {
        var copy = new Dictionary<string, CheckedType>(_mapping);
        if (!InternalIsAssignableList(copy, l1, l2))
            return false;
        _mapping = copy;
        return true;
    }

    private void SetType(CelExpr e, CheckedType type) => _types[e] = type;

    private CheckedType? GetType(CelExpr e) => _types.GetValueOrDefault(e);

    private void AssertType(CelExpr e, CheckedType type)
    {
        var actual = GetType(e)!;
        if (!IsAssignable(type, actual))
            TypeMismatch(e.Location, type, actual);
    }

    private void TypeMismatch(CelLocation location, CheckedType expected, CheckedType actual) =>
        _errors.Report(location, $"expected type '{expected.Format()}' but found '{actual.Format()}'");

    private void UndeclaredReference(CelLocation location, string name) =>
        _errors.Report(location, $"undeclared reference to '{name}' (in container '')");

    private CheckedType? LookupFieldType(CelLocation location, string structType, string fieldName)
    {
        if (!CelDeclarations.StructTypes.TryGetValue(structType, out var fields))
        {
            _errors.Report(location, $"unexpected failed resolution of '{structType}'");
            return null;
        }

        if (fields.TryGetValue(fieldName, out var fieldType))
            return fieldType;

        _errors.Report(location, $"undefined field '{fieldName}'");
        return null;
    }

    /// <summary>cel-go's <c>Env.LookupIdent</c> in the empty container.</summary>
    private CheckedType? LookupIdent(string name)
    {
        var candidate = ResolveCandidateName(name);
        for (var i = _scopes.Count - 1; i >= 0; i--)
        {
            if (_scopes[i].TryGetValue(candidate, out var type))
                return type;
        }

        if (CelDeclarations.StructTypes.ContainsKey(candidate))
            return CheckedType.TypeOf(CheckedType.Object(candidate));
        if (CelDeclarations.RegistryTypeIdentifiers.TryGetValue(candidate, out var registryType))
            return registryType;
        if (CelDeclarations.EnumValueIdentifiers.Contains(candidate))
            return CheckedType.Int;
        return null;
    }

    private static CelFunction? LookupFunction(string name) =>
        CelDeclarations.Functions.GetValueOrDefault(ResolveCandidateName(name));

    /// <summary>In the empty container a name's only candidate is itself, a leading dot rooting it.</summary>
    private static string ResolveCandidateName(string name) => name.StartsWith('.') ? name[1..] : name;

    /// <summary>cel-go's <c>containers.ToQualifiedName</c>.</summary>
    private static string? ToQualifiedName(CelExpr e) =>
        e switch
        {
            CelIdent ident => ident.Name,
            CelSelect { TestOnly: false } select when ToQualifiedName(select.Operand) is { } prefix =>
                prefix + "." + select.Field,
            _ => null,
        };

    private static bool IsWellKnownType(CheckedType t) =>
        t.Kind switch
        {
            CheckedKind.Any or CheckedKind.Timestamp or CheckedKind.Duration or CheckedKind.Dyn or CheckedKind.Null => true,
            CheckedKind.Bool or CheckedKind.Bytes or CheckedKind.Double or CheckedKind.Int or CheckedKind.String
                or CheckedKind.Uint => t.IsAssignableType(CheckedType.Null),
            CheckedKind.List => ReferenceEquals(t.Parameters[0], CheckedType.Dyn),
            CheckedKind.Map => ReferenceEquals(t.Parameters[0], CheckedType.String)
                && ReferenceEquals(t.Parameters[1], CheckedType.Dyn),
            _ => false,
        };

    private static string WellKnownTypeName(CheckedType t) =>
        t.Kind switch
        {
            CheckedKind.Any => "google.protobuf.Any",
            CheckedKind.Bool => "google.protobuf.BoolValue",
            CheckedKind.Bytes => "google.protobuf.BytesValue",
            CheckedKind.Double => "google.protobuf.DoubleValue",
            CheckedKind.Duration => "google.protobuf.Duration",
            CheckedKind.Dyn => "google.protobuf.Value",
            CheckedKind.Int => "google.protobuf.Int64Value",
            CheckedKind.List => "google.protobuf.ListValue",
            CheckedKind.Null => "google.protobuf.NullValue",
            CheckedKind.Map => "google.protobuf.Struct",
            CheckedKind.String => "google.protobuf.StringValue",
            CheckedKind.Timestamp => "google.protobuf.Timestamp",
            CheckedKind.Uint => "google.protobuf.UInt64Value",
            _ => string.Empty,
        };

    // ---- checker/types.go ----

    private static bool IsDyn(CheckedType t) => t.Kind is CheckedKind.Dyn or CheckedKind.Any;

    private static bool IsError(CheckedType t) => t.Kind == CheckedKind.Error;

    private static bool IsDynOrError(CheckedType t) => IsError(t) || IsDyn(t);

    private static (CheckedType Type, bool IsOptional) MaybeUnwrapOptional(CheckedType t) =>
        t.Kind == CheckedKind.Opaque && t.Name == "optional_type" ? (t.Parameters[0], true) : (t, false);

    private static bool IsEqualOrLessSpecific(CheckedType t1, CheckedType t2)
    {
        if (IsDyn(t1) || t1.Kind == CheckedKind.TypeParam)
            return true;
        if (IsDyn(t2) || t2.Kind == CheckedKind.TypeParam)
            return false;
        if (t1.Kind != t2.Kind)
            return false;

        switch (t1.Kind)
        {
            case CheckedKind.Opaque:
                if (t1.Name != t2.Name || t1.Parameters.Count != t2.Parameters.Count)
                    return false;
                for (var i = 0; i < t1.Parameters.Count; i++)
                {
                    if (!IsEqualOrLessSpecific(t1.Parameters[i], t2.Parameters[i]))
                        return false;
                }
                return true;
            case CheckedKind.List:
                return IsEqualOrLessSpecific(t1.Parameters[0], t2.Parameters[0]);
            case CheckedKind.Map:
                return IsEqualOrLessSpecific(t1.Parameters[0], t2.Parameters[0])
                    && IsEqualOrLessSpecific(t1.Parameters[1], t2.Parameters[1]);
            case CheckedKind.Type:
                return true;
            default:
                return t1.IsExactType(t2);
        }
    }

    private static bool InternalIsAssignable(Dictionary<string, CheckedType> m, CheckedType t1, CheckedType t2)
    {
        if (t2.Kind == CheckedKind.TypeParam)
        {
            var (valid, t2HasSub) = IsValidTypeSubstitution(m, t1, t2);
            if (valid)
                return true;
            if (t2HasSub)
                return false;
        }

        if (t1.Kind == CheckedKind.TypeParam)
            return IsValidTypeSubstitution(m, t2, t1).Valid;

        if (IsDynOrError(t1) || IsDynOrError(t2))
            return true;
        if (t1.Kind == CheckedKind.Null)
            return InternalIsAssignableNull(t2);
        if (t2.Kind == CheckedKind.Null)
            return InternalIsAssignableNull(t1);

        switch (t1.Kind)
        {
            case CheckedKind.Bool:
            case CheckedKind.Bytes:
            case CheckedKind.Double:
            case CheckedKind.Int:
            case CheckedKind.String:
            case CheckedKind.Uint:
            case CheckedKind.Any:
            case CheckedKind.Duration:
            case CheckedKind.Timestamp:
            case CheckedKind.Struct:
                return t2.IsAssignableType(t1);
            case CheckedKind.Type:
                return t2.Kind == CheckedKind.Type;
            case CheckedKind.Opaque:
            case CheckedKind.List:
            case CheckedKind.Map:
                return t1.Kind == t2.Kind && t1.Name == t2.Name
                    && InternalIsAssignableList(m, t1.Parameters, t2.Parameters);
            default:
                return false;
        }
    }

    private static (bool Valid, bool HasSub) IsValidTypeSubstitution(
        Dictionary<string, CheckedType> m, CheckedType t1, CheckedType t2)
    {
        if (t1.Kind == t2.Kind && t1.IsExactType(t2))
            return (true, true);

        if (m.TryGetValue(t2.Format(), out var t2Sub))
        {
            if (t1.Kind == t2Sub.Kind && t1.IsExactType(t2Sub))
                return (true, true);
            if (InternalIsAssignable(m, t1, t2Sub))
            {
                var t2New = MostGeneral(t1, t2Sub);
                if (NotReferencedIn(m, t2, t2New))
                    m[t2.Format()] = t2New;
                return (true, true);
            }

            return (false, true);
        }

        if (NotReferencedIn(m, t2, t1))
        {
            m[t2.Format()] = t1;
            return (true, false);
        }

        return (false, false);
    }

    private static bool InternalIsAssignableList(
        Dictionary<string, CheckedType> m, IReadOnlyList<CheckedType> l1, IReadOnlyList<CheckedType> l2)
    {
        if (l1.Count != l2.Count)
            return false;
        for (var i = 0; i < l1.Count; i++)
        {
            if (!InternalIsAssignable(m, l1[i], l2[i]))
                return false;
        }
        return true;
    }

    private static bool InternalIsAssignableNull(CheckedType t) =>
        t.Kind is CheckedKind.Opaque or CheckedKind.Struct or CheckedKind.Any or CheckedKind.Duration
            or CheckedKind.Timestamp
        || t.IsAssignableType(CheckedType.Null);

    private static CheckedType MostGeneral(CheckedType t1, CheckedType t2) =>
        IsEqualOrLessSpecific(t1, t2) ? t1 : t2;

    private static bool NotReferencedIn(Dictionary<string, CheckedType> m, CheckedType t, CheckedType withinType)
    {
        if (t.IsExactType(withinType))
            return false;

        switch (withinType.Kind)
        {
            case CheckedKind.TypeParam:
                return !m.TryGetValue(withinType.Format(), out var sub) || NotReferencedIn(m, t, sub);
            case CheckedKind.Opaque:
            case CheckedKind.List:
            case CheckedKind.Map:
            case CheckedKind.Type:
                return withinType.Parameters.All(p => NotReferencedIn(m, t, p));
            default:
                return true;
        }
    }

    /// <summary>Replaces bound type parameters; unbound ones become dyn when <paramref name="typeParamToDyn"/>.</summary>
    private static CheckedType Substitute(Dictionary<string, CheckedType> m, CheckedType t, bool typeParamToDyn)
    {
        if (m.TryGetValue(t.Format(), out var sub))
            return Substitute(m, sub, typeParamToDyn);
        if (typeParamToDyn && t.Kind == CheckedKind.TypeParam)
            return CheckedType.Dyn;

        return t.Kind switch
        {
            CheckedKind.Opaque => CheckedType.Opaque(t.Name, t.Parameters.Select(p => Substitute(m, p, typeParamToDyn)).ToArray()),
            CheckedKind.List => CheckedType.List(Substitute(m, t.Parameters[0], typeParamToDyn)),
            CheckedKind.Map => CheckedType.Map(
                Substitute(m, t.Parameters[0], typeParamToDyn), Substitute(m, t.Parameters[1], typeParamToDyn)),
            CheckedKind.Type when t.Parameters.Count > 0 => CheckedType.TypeOf(Substitute(m, t.Parameters[0], typeParamToDyn)),
            _ => t,
        };
    }
}
