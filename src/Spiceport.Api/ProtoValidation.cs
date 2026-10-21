using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Validate;

namespace Spiceport.Api;

/// <summary>
/// The protoc-gen-validate (PGV) request contract of the vendored <c>authzed.api.v1</c> protos, read from
/// their <c>(validate.rules)</c> annotations at run time. SpiceDB's first interceptor, go-grpc-middleware's
/// <c>validator</c>, calls the <c>ValidateAll</c> method PGV generated into authzed-go for every request
/// message and rejects a violation with <c>INVALID_ARGUMENT</c> and <c>err.Error()</c> as the detail.
/// <see cref="ValidateAll"/> reproduces that method generically: the same checks, in the same order, with the
/// same text.
/// </summary>
/// <remarks>
/// <para>
/// The order follows PGV's Go message template: the fields outside any oneof in declaration order, then
/// each oneof, then proto3 <c>optional</c> fields. Each violation reads
/// <c>invalid {GoMessage}.{GoField}: {reason}</c>; a nested message's own violations follow
/// <c>embedded message failed validation | caused by: </c>, and the violations of one message are joined
/// with <c>"; "</c> (PGV's <c>MultiError</c>).
/// </para>
/// <para>
/// A rule this class does not implement throws <see cref="NotSupportedException"/> when the message's plan is
/// first built, so a vendored proto that grows a new kind of rule fails loudly in tests rather than being
/// silently unenforced.
/// </para>
/// </remarks>
internal static class ProtoValidation
{
    private const string EmbeddedFailure = "embedded message failed validation";

    private static readonly ConcurrentDictionary<MessageDescriptor, Plan> Plans = new();

    /// <summary>
    /// Returns PGV's <c>ValidateAll()</c> error text for <paramref name="message"/>, or <c>null</c> when the
    /// message satisfies every rule.
    /// </summary>
    internal static string? ValidateAll(IMessage message)
    {
        var violations = Violations(message);
        return violations.Count == 0 ? null : string.Join("; ", violations);
    }

    /// <summary>
    /// Builds the plan for <paramref name="descriptor"/> and every message reachable from it, throwing
    /// <see cref="NotSupportedException"/> for a rule this class does not implement.
    /// </summary>
    internal static void EnsureSupported(MessageDescriptor descriptor)
    {
        var visited = new HashSet<MessageDescriptor>();
        var pending = new Stack<MessageDescriptor>([descriptor]);
        while (pending.TryPop(out var next))
        {
            if (!visited.Add(next) || !HasValidator(next))
                continue;
            PlanFor(next);
            foreach (var field in next.Fields.InDeclarationOrder())
            {
                if (field.FieldType is FieldType.Message or FieldType.Group)
                    pending.Push(field.MessageType);
            }
        }
    }

    private static List<string> Violations(IMessage message)
    {
        var violations = new List<string>();
        PlanFor(message.Descriptor).Run(message, violations);
        return violations;
    }

    /// <summary>
    /// The cached plan for <paramref name="descriptor"/>. A plan holds only its own message's checks; an
    /// embedded message's plan is looked up when that message is validated, so a recursive message type
    /// needs no special handling.
    /// </summary>
    private static Plan PlanFor(MessageDescriptor descriptor) => Plans.GetOrAdd(descriptor, d => new Plan(d));

    /// <summary>
    /// Whether PGV generated a validator for <paramref name="descriptor"/>'s Go type. The well-known
    /// <c>google.protobuf</c> types have none, so an embedded one is never validated.
    /// </summary>
    private static bool HasValidator(MessageDescriptor descriptor) =>
        descriptor.File.Package != "google.protobuf"
        && !(descriptor.GetOptions()?.GetExtension(ValidateExtensions.Ignored) ?? false);

    /// <summary>One message's checks, in PGV's order.</summary>
    private sealed class Plan
    {
        private readonly MessageDescriptor descriptor;
        private readonly List<Action<IMessage, List<string>>> checks;

        internal Plan(MessageDescriptor descriptor)
        {
            this.descriptor = descriptor;
            GoMessage = GoMessageName(descriptor);
            checks = Build();
        }

        private string GoMessage { get; }

        private List<Action<IMessage, List<string>>> Build()
        {
            var built = new List<Action<IMessage, List<string>>>();
            if (descriptor.GetOptions()?.GetExtension(ValidateExtensions.Disabled) ?? false)
                return built;

            foreach (var field in descriptor.Fields.InDeclarationOrder())
            {
                if (field.ContainingOneof is null)
                    AddFieldChecks(built, field, presenceGuarded: false);
            }

            foreach (var oneof in descriptor.Oneofs)
            {
                if (!oneof.IsSynthetic)
                    AddOneofChecks(built, oneof);
            }

            foreach (var oneof in descriptor.Oneofs)
            {
                if (oneof.IsSynthetic)
                    AddFieldChecks(built, oneof.Fields[0], presenceGuarded: true);
            }

            return built;
        }


        internal void Run(IMessage message, List<string> violations)
        {
            foreach (var check in checks)
                check(message, violations);
        }

        private string Violation(string field, string reason) => $"invalid {GoMessage}.{field}: {reason}";

        private string Embedded(string field, List<string> causes) =>
            Violation(field, $"{EmbeddedFailure} | caused by: {string.Join("; ", causes)}");

        private void AddOneofChecks(List<Action<IMessage, List<string>>> checks, OneofDescriptor oneof)
        {
            var perField = new Dictionary<int, List<Action<IMessage, List<string>>>>();
            foreach (var field in oneof.Fields)
                perField[field.FieldNumber] = ValueChecks(field, inOneof: true);

            var required = oneof.GetOptions()?.GetExtension(ValidateExtensions.Required) ?? false;
            var oneofName = GoName(oneof.Name);
            checks.Add((message, violations) =>
            {
                var set = oneof.Accessor.GetCaseFieldDescriptor(message);
                if (set is not null)
                {
                    foreach (var check in perField[set.FieldNumber])
                        check(message, violations);
                }
                else if (required)
                {
                    violations.Add(Violation(oneofName, "value is required"));
                }
            });
        }

        private void AddFieldChecks(
            List<Action<IMessage, List<string>>> checks, FieldDescriptor field, bool presenceGuarded)
        {
            var fieldChecks = ValueChecks(field, inOneof: false);
            if (fieldChecks.Count == 0)
                return;

            if (!presenceGuarded)
            {
                checks.AddRange(fieldChecks);
                return;
            }

            checks.Add((message, violations) =>
            {
                if (!field.Accessor.HasValue(message))
                    return;
                foreach (var check in fieldChecks)
                    check(message, violations);
            });
        }

        /// <summary>The checks for one field's value, rendered as PGV renders its rule type.</summary>
        private List<Action<IMessage, List<string>>> ValueChecks(FieldDescriptor field, bool inOneof)
        {
            var rules = field.GetOptions()?.GetExtension(ValidateExtensions.Rules);
            var name = GoName(field.Name);
            var result = new List<Action<IMessage, List<string>>>();

            if (field.IsMap)
            {
                if (rules is not null || field.MessageType.FindFieldByNumber(2).FieldType == FieldType.Message)
                    throw Unsupported(field, "map");
                return result;
            }

            if (field.IsRepeated)
            {
                AddRepeatedChecks(field, name, rules, result);
                return result;
            }

            var element = ElementChecks(field, rules, inOneof);
            if (element is not null)
                result.Add((message, violations) => element(field.Accessor.GetValue(message), name, violations));
            return result;
        }

        private void AddRepeatedChecks(
            FieldDescriptor field, string name, FieldRules? rules, List<Action<IMessage, List<string>>> result)
        {
            RepeatedRules? repeated = null;
            if (rules is not null)
            {
                if (rules.TypeCase != FieldRules.TypeOneofCase.Repeated)
                    throw Unsupported(field, rules.TypeCase.ToString());
                repeated = rules.Repeated;
            }

            if (repeated is not null && (repeated.IgnoreEmpty || repeated.Unique))
                throw Unsupported(field, "repeated ignore_empty/unique");

            var minItems = repeated?.MinItems ?? 0;
            var maxItems = repeated?.MaxItems ?? 0;
            if (minItems != 0 || maxItems != 0)
            {
                result.Add((message, violations) =>
                {
                    var count = ((System.Collections.IList)field.Accessor.GetValue(message)).Count;
                    var reason = ItemCountViolation((ulong)count, minItems, maxItems);
                    if (reason is not null)
                        violations.Add(Violation(name, reason));
                });
            }

            var element = ElementChecks(field, repeated?.Items, inOneof: false);
            if (element is null)
                return;

            result.Add((message, violations) =>
            {
                var items = (System.Collections.IList)field.Accessor.GetValue(message);
                for (var i = 0; i < items.Count; i++)
                    element(items[i], $"{name}[{i.ToString(CultureInfo.InvariantCulture)}]", violations);
            });
        }

        private static string? ItemCountViolation(ulong count, ulong min, ulong max)
        {
            if (min != 0)
            {
                if (min == max)
                    return count != min ? $"value must contain exactly {min} item(s)" : null;
                if (max != 0)
                    return count < min || count > max ? $"value must contain between {min} and {max} items, inclusive" : null;
                return count < min ? $"value must contain at least {min} item(s)" : null;
            }

            return max != 0 && count > max ? $"value must contain no more than {max} item(s)" : null;
        }

        /// <summary>
        /// The checks for a single value of <paramref name="field"/>'s element type, or <c>null</c> when there
        /// are none. The returned check reports under the field label it is given (<c>Field</c> or
        /// <c>Field[i]</c>).
        /// </summary>
        private Action<object?, string, List<string>>? ElementChecks(FieldDescriptor field, FieldRules? rules, bool inOneof)
        {
            if (field.FieldType is FieldType.Message or FieldType.Group)
                return MessageChecks(field, rules, inOneof);

            if (rules is null || rules.TypeCase == FieldRules.TypeOneofCase.None)
                return null;

            return rules.TypeCase switch
            {
                FieldRules.TypeOneofCase.String => StringChecks(field, rules.String),
                FieldRules.TypeOneofCase.Enum => EnumChecks(field, rules.Enum),
                FieldRules.TypeOneofCase.Bool => BoolChecks(field, rules.Bool),
                FieldRules.TypeOneofCase.Uint32 => NumberChecks(field, rules.Uint32),
                _ => throw Unsupported(field, rules.TypeCase.ToString()),
            };
        }

        private Action<object?, string, List<string>>? MessageChecks(FieldDescriptor field, FieldRules? rules, bool inOneof)
        {
            // PGV keeps a message field's own rules outside FieldRules.type; a type rule here would be a
            // well-known-type rule (Any, Duration, Timestamp), which this class does not implement.
            if (rules is not null && rules.TypeCase != FieldRules.TypeOneofCase.None)
                throw Unsupported(field, rules.TypeCase.ToString());
            var required = rules?.Message?.Required ?? false;
            var skip = rules?.Message?.Skip ?? false;

            // A message set in a oneof is present by construction: PGV's required check cannot fire there.
            required &= !inOneof;

            var embedded = !skip && HasValidator(field.MessageType);

            if (!required && !embedded)
                return null;

            return (value, label, violations) =>
            {
                if (value is null)
                {
                    if (required)
                        violations.Add(Violation(label, "value is required"));
                    return;
                }

                if (!embedded)
                    return;

                var causes = Violations((IMessage)value);
                if (causes.Count > 0)
                    violations.Add(Embedded(label, causes));
            };
        }

        private Action<object?, string, List<string>>? StringChecks(FieldDescriptor field, StringRules r)
        {
            if (r.IgnoreEmpty || r.WellKnownCase != StringRules.WellKnownOneofCase.None)
                throw Unsupported(field, "string ignore_empty/well-known format");

            var tests = new List<Func<string, string?>>();

            if (r.HasConst)
                tests.Add(s => s != r.Const ? $"value must equal {r.Const}" : null);

            if (r.In.Count > 0)
                tests.Add(s => r.In.Contains(s) ? null : $"value must be in list {GoList(r.In)}");
            else if (r.NotIn.Count > 0)
                tests.Add(s => r.NotIn.Contains(s) ? $"value must not be in list {GoList(r.NotIn)}" : null);

            if (r.HasLen || (r.HasMinLen && r.HasMaxLen && r.MinLen == r.MaxLen))
            {
                var exact = r.HasLen ? r.Len : r.MinLen;
                tests.Add(s => (ulong)Runes(s) != exact ? $"value length must be {exact} runes" : null);
            }
            else if (r.HasMinLen)
            {
                tests.Add(r.HasMaxLen
                    ? s => (ulong)Runes(s) < r.MinLen || (ulong)Runes(s) > r.MaxLen
                        ? $"value length must be between {r.MinLen} and {r.MaxLen} runes, inclusive"
                        : null
                    : s => (ulong)Runes(s) < r.MinLen ? $"value length must be at least {r.MinLen} runes" : null);
            }
            else if (r.HasMaxLen)
            {
                tests.Add(s => (ulong)Runes(s) > r.MaxLen ? $"value length must be at most {r.MaxLen} runes" : null);
            }

            if (r.HasLenBytes || (r.HasMinBytes && r.HasMaxBytes && r.MinBytes == r.MaxBytes))
            {
                var exact = r.HasLenBytes ? r.LenBytes : r.MinBytes;
                tests.Add(s => (ulong)Bytes(s) != exact ? $"value length must be {exact} bytes" : null);
            }
            else if (r.HasMinBytes)
            {
                tests.Add(r.HasMaxBytes
                    ? s => (ulong)Bytes(s) < r.MinBytes || (ulong)Bytes(s) > r.MaxBytes
                        ? $"value length must be between {r.MinBytes} and {r.MaxBytes} bytes, inclusive"
                        : null
                    : s => (ulong)Bytes(s) < r.MinBytes ? $"value length must be at least {r.MinBytes} bytes" : null);
            }
            else if (r.HasMaxBytes)
            {
                tests.Add(s => (ulong)Bytes(s) > r.MaxBytes ? $"value length must be at most {r.MaxBytes} bytes" : null);
            }

            if (r.HasPrefix)
                tests.Add(s => s.StartsWith(r.Prefix, StringComparison.Ordinal) ? null : $"value does not have prefix {GoQuote(r.Prefix)}");
            if (r.HasSuffix)
                tests.Add(s => s.EndsWith(r.Suffix, StringComparison.Ordinal) ? null : $"value does not have suffix {GoQuote(r.Suffix)}");
            if (r.HasContains)
                tests.Add(s => s.Contains(r.Contains, StringComparison.Ordinal) ? null : $"value does not contain substring {GoQuote(r.Contains)}");
            if (r.HasNotContains)
                tests.Add(s => s.Contains(r.NotContains, StringComparison.Ordinal) ? $"value contains substring {GoQuote(r.NotContains)}" : null);

            if (r.HasPattern)
            {
                var regex = GoRegex(r.Pattern);
                var reason = $"value does not match regex pattern {GoQuote(r.Pattern)}";
                tests.Add(s => regex.IsMatch(s) ? null : reason);
            }

            return Scalar<string>(tests);
        }

        private Action<object?, string, List<string>>? EnumChecks(FieldDescriptor field, EnumRules r)
        {
            var type = field.EnumType;
            string NameOf(int number) => type.FindValueByNumber(number)?.Name ?? number.ToString(CultureInfo.InvariantCulture);

            var tests = new List<Func<int, string?>>();
            if (r.HasConst)
                tests.Add(v => v != r.Const ? $"value must equal {NameOf(r.Const)}" : null);
            if (r.In.Count > 0)
                tests.Add(v => r.In.Contains(v) ? null : $"value must be in list [{string.Join(" ", r.In.Select(NameOf))}]");
            else if (r.NotIn.Count > 0)
                tests.Add(v => r.NotIn.Contains(v) ? $"value must not be in list [{string.Join(" ", r.NotIn.Select(NameOf))}]" : null);
            if (r.DefinedOnly)
                tests.Add(v => type.FindValueByNumber(v) is null ? "value must be one of the defined enum values" : null);

            // The C# accessor returns the enum as its CLR type (or a boxed int for an unknown value).
            return Scalar<object>(tests.Select<Func<int, string?>, Func<object, string?>>(t => v => t(Convert.ToInt32(v, CultureInfo.InvariantCulture))).ToList());
        }

        private Action<object?, string, List<string>>? BoolChecks(FieldDescriptor field, BoolRules r)
        {
            if (!r.HasConst)
                return null;
            var expected = r.Const ? "true" : "false";
            return Scalar<bool>([v => v != r.Const ? $"value must equal {expected}" : null]);
        }

        private Action<object?, string, List<string>>? NumberChecks(FieldDescriptor field, UInt32Rules r)
        {
            if (r.IgnoreEmpty || (r.HasLt || r.HasLte) && (r.HasGt || r.HasGte))
                throw Unsupported(field, "uint32 ignore_empty/range");

            var tests = new List<Func<uint, string?>>();
            if (r.HasConst)
                tests.Add(v => v != r.Const ? $"value must equal {r.Const}" : null);
            if (r.HasLt)
                tests.Add(v => v >= r.Lt ? $"value must be less than {r.Lt}" : null);
            else if (r.HasLte)
                tests.Add(v => v > r.Lte ? $"value must be less than or equal to {r.Lte}" : null);
            else if (r.HasGt)
                tests.Add(v => v <= r.Gt ? $"value must be greater than {r.Gt}" : null);
            else if (r.HasGte)
                tests.Add(v => v < r.Gte ? $"value must be greater than or equal to {r.Gte}" : null);
            if (r.In.Count > 0)
                tests.Add(v => r.In.Contains(v) ? null : $"value must be in list [{string.Join(" ", r.In)}]");
            else if (r.NotIn.Count > 0)
                tests.Add(v => r.NotIn.Contains(v) ? $"value must not be in list [{string.Join(" ", r.NotIn)}]" : null);

            return Scalar<uint>(tests);
        }

        private Action<object?, string, List<string>>? Scalar<T>(List<Func<T, string?>> tests)
        {
            if (tests.Count == 0)
                return null;
            return (value, label, violations) =>
            {
                foreach (var test in tests)
                {
                    var reason = test((T)value!);
                    if (reason is not null)
                        violations.Add(Violation(label, reason));
                }
            };
        }

        private NotSupportedException Unsupported(FieldDescriptor field, string rule) =>
            new($"protoc-gen-validate rule '{rule}' on {descriptor.FullName}.{field.Name} is not implemented");
    }

    private static int Runes(string s)
    {
        var count = 0;
        foreach (var _ in s.EnumerateRunes())
            count++;
        return count;
    }

    private static int Bytes(string s) => Encoding.UTF8.GetByteCount(s);

    /// <summary>Go's <c>fmt.Sprint</c> of a <c>[]string</c>: the items space-separated in brackets.</summary>
    private static string GoList(IEnumerable<string> items) => "[" + string.Join(" ", items) + "]";

    /// <summary>
    /// Compiles a PGV (Go RE2) pattern for .NET. RE2's <c>$</c> without the <c>m</c> flag matches only at the
    /// very end of the text, where .NET's also matches before a final newline, so an unescaped <c>$</c>
    /// outside a character class becomes <c>\z</c>.
    /// </summary>
    internal static Regex GoRegex(string pattern)
    {
        var translated = new StringBuilder(pattern.Length + 4);
        var inClass = false;
        for (var i = 0; i < pattern.Length; i++)
        {
            var c = pattern[i];
            if (c == '\\' && i + 1 < pattern.Length)
            {
                translated.Append(c).Append(pattern[++i]);
                continue;
            }

            if (inClass)
            {
                if (c == ']')
                    inClass = false;
                translated.Append(c);
                continue;
            }

            switch (c)
            {
                case '[':
                    inClass = true;
                    translated.Append(c);
                    // A ']' (after an optional '^') right after '[' is a literal member, not the class end.
                    if (i + 1 < pattern.Length && pattern[i + 1] == '^')
                        translated.Append(pattern[++i]);
                    if (i + 1 < pattern.Length && pattern[i + 1] == ']')
                        translated.Append(pattern[++i]);
                    break;
                case '$':
                    translated.Append(@"\z");
                    break;
                default:
                    translated.Append(c);
                    break;
            }
        }

        return new Regex(translated.ToString(), RegexOptions.CultureInvariant);
    }

    /// <summary>Go's <c>strconv.Quote</c>, which PGV's <c>%q</c> applies to a pattern or literal in a reason.</summary>
    internal static string GoQuote(string s)
    {
        var quoted = new StringBuilder(s.Length + 2).Append('"');
        foreach (var rune in s.EnumerateRunes())
        {
            switch (rune.Value)
            {
                case '"': quoted.Append("\\\""); break;
                case '\\': quoted.Append("\\\\"); break;
                case '\a': quoted.Append("\\a"); break;
                case '\b': quoted.Append("\\b"); break;
                case '\f': quoted.Append("\\f"); break;
                case '\n': quoted.Append("\\n"); break;
                case '\r': quoted.Append("\\r"); break;
                case '\t': quoted.Append("\\t"); break;
                case '\v': quoted.Append("\\v"); break;
                default:
                    if (IsGoPrintable(rune))
                        quoted.Append(rune.ToString());
                    else if (rune.Value < ' ' || rune.Value == 0x7f)
                        quoted.Append(CultureInfo.InvariantCulture, $"\\x{rune.Value:x2}");
                    else if (rune.Value < 0x10000)
                        quoted.Append(CultureInfo.InvariantCulture, $"\\u{rune.Value:x4}");
                    else
                        quoted.Append(CultureInfo.InvariantCulture, $"\\U{rune.Value:x8}");
                    break;
            }
        }

        return quoted.Append('"').ToString();
    }

    private static bool IsGoPrintable(Rune rune) =>
        rune.Value == ' ' || Rune.GetUnicodeCategory(rune) switch
        {
            UnicodeCategory.Control or UnicodeCategory.Format or UnicodeCategory.Surrogate
                or UnicodeCategory.PrivateUse or UnicodeCategory.OtherNotAssigned
                or UnicodeCategory.SpaceSeparator or UnicodeCategory.LineSeparator
                or UnicodeCategory.ParagraphSeparator => false,
            _ => true,
        };

    /// <summary>
    /// The Go type name protoc-gen-go gives a message: a nested message is its parent's name, <c>_</c>, and
    /// its own (<c>SubjectFilter_RelationFilter</c>).
    /// </summary>
    private static string GoMessageName(MessageDescriptor descriptor) =>
        descriptor.ContainingType is null
            ? GoName(descriptor.Name)
            : GoMessageName(descriptor.ContainingType) + "_" + GoName(descriptor.Name);

    /// <summary>protoc-gen-go's <c>GoCamelCase</c>, which names the Go field (and PGV's error label).</summary>
    internal static string GoName(string name)
    {
        var b = new StringBuilder(name.Length);
        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (c == '.' && i + 1 < name.Length && char.IsAsciiLetterLower(name[i + 1]))
                continue;
            if (c == '.')
            {
                b.Append('_');
            }
            else if (c == '_' && (i == 0 || name[i - 1] == '.'))
            {
                b.Append('X');
            }
            else if (c == '_' && i + 1 < name.Length && char.IsAsciiLetterLower(name[i + 1]))
            {
                // Dropped; the following lower-case letter is capitalised below.
            }
            else if (char.IsAsciiDigit(c))
            {
                b.Append(c);
            }
            else
            {
                b.Append(char.IsAsciiLetterLower(c) ? char.ToUpperInvariant(c) : c);
                for (; i + 1 < name.Length && char.IsAsciiLetterLower(name[i + 1]); i++)
                    b.Append(name[i + 1]);
            }
        }

        return b.ToString();
    }
}
