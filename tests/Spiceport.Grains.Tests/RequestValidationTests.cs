using Google.Protobuf;
using Google.Protobuf.Reflection;
using Grpc.Core;
using Spiceport.Api;
using V1 = Authzed.Api.V1;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Pure tests (no cluster) for <see cref="RequestValidation"/>: the protoc-gen-validate rules annotated on the
/// vendored <c>authzed.api.v1</c> protos and authzed-go's handwritten rules, which SpiceDB enforces in its
/// interceptors before any handler runs. Each expected detail is the exact text SpiceDB v1.49.2 returns: the
/// output of authzed-go v1.7.0's generated <c>ValidateAll</c> (then <c>HandwrittenValidate</c>) for the same
/// message. Requests are written as protobuf JSON so the field paths read as they do on the wire.
/// </summary>
public class RequestValidationTests
{
    private const string Embedded = "embedded message failed validation | caused by: ";
    private static readonly string TypePattern = Pattern("""^([a-z][a-z0-9_]{1,61}[a-z0-9]/)*[a-z][a-z0-9_]{1,62}[a-z0-9]$""");
    private static readonly string OptionalTypePattern = Pattern("""^(([a-z][a-z0-9_]{1,61}[a-z0-9]/)*[a-z][a-z0-9_]{1,62}[a-z0-9])?$""");
    private static readonly string IdPattern = Pattern("""^(([a-zA-Z0-9/_|\\-=+]{1,})|\\*)$""");
    private static readonly string FilterIdPattern = Pattern("""^([a-zA-Z0-9/_|\\-=+]{1,})?$""");
    private static readonly string RelationPattern = Pattern("""^[a-z][a-z0-9_]{1,62}[a-z0-9]$""");
    private static readonly string OptionalRelationPattern = Pattern("""^([a-z][a-z0-9_]{1,62}[a-z0-9])?$""");
    private static readonly string CaveatPattern = Pattern("""^([a-zA-Z0-9_][a-zA-Z0-9/_|-]{0,127})$""");

    /// <summary>PGV's reason for a pattern mismatch: the pattern as Go's <c>%q</c> prints it.</summary>
    private static string Pattern(string goQuoted) => $"value does not match regex pattern \"{goQuoted}\"";

    private const string Alice = """{"object":{"objectType":"user","objectId":"alice"}}""";

    private static T Parse<T>(string json) where T : IMessage<T>, new() => JsonParser.Default.Parse<T>(json);

    private static string Check(string resource, string permission = "view", string subject = Alice, string consistency = "") =>
        $$"""{{{consistency}}"resource":{{resource}},"permission":"{{permission}}","subject":{{subject}}}""";

    private static string Doc(string id) => $$"""{"objectType":"document","objectId":"{{id}}"}""";

    private static string Update(string relationship, string operation = "\"OPERATION_TOUCH\"") =>
        $$"""{"operation":{{operation}},"relationship":{{relationship}}}""";

    private static string Rel(string resource, string relation = "viewer", string subject = Alice, string extra = "") =>
        $$"""{"resource":{{resource}},"relation":"{{relation}}","subject":{{subject}}{{extra}}}""";

    private static void AssertRejected(IMessage request, string detail) =>
        AssertRejected(request, StatusCode.InvalidArgument, detail);

    private static void AssertRejected(IMessage request, StatusCode code, string detail)
    {
        var ex = Assert.Throws<RpcException>(() => RequestValidation.Validate(request));
        Assert.Equal(code, ex.StatusCode);
        Assert.Equal(detail, ex.Status.Detail);
    }

    /// <summary>
    /// The protoc-gen-validate layer alone. SpiceDB's consistency middleware runs ahead of it and rejects
    /// some of these requests first, so what the RPC returns is asserted separately.
    /// </summary>
    private static void AssertPgv(IMessage request, string detail) =>
        Assert.Equal(detail, ProtoValidation.ValidateAll(request));

    [Fact]
    public void Valid_requests_pass()
    {
        RequestValidation.Validate(Parse<V1::CheckPermissionRequest>(Check(
            """{"objectType":"org/document","objectId":"a/b_c|d-e=f+g"}""",
            subject: """{"object":{"objectType":"user","objectId":"*"}}""")));
        RequestValidation.Validate(Parse<V1::ReadRelationshipsRequest>("""{"relationshipFilter":{"resourceType":""}}"""));
        RequestValidation.Validate(Parse<V1::ReadSchemaRequest>("{}"));
        RequestValidation.Validate(Parse<V1::WriteSchemaRequest>("""{"schema":""}"""));
    }

    [Fact]
    public void Object_type_and_id_must_match_their_grammar() =>
        AssertRejected(
            Parse<V1::CheckPermissionRequest>(Check("""{"objectType":"Document","objectId":"doc 1"}""")),
            $"invalid CheckPermissionRequest.Resource: {Embedded}invalid ObjectReference.ObjectType: {TypePattern}; " +
            $"invalid ObjectReference.ObjectId: {IdPattern}");

    [Fact]
    public void A_trailing_newline_does_not_satisfy_an_anchored_pattern() =>
        AssertRejected(
            Parse<V1::CheckPermissionRequest>(Check("""{"objectType":"document\n","objectId":"doc1"}""")),
            $"invalid CheckPermissionRequest.Resource: {Embedded}invalid ObjectReference.ObjectType: {TypePattern}");

    [Fact]
    public void A_non_ascii_object_id_is_rejected() =>
        AssertRejected(
            Parse<V1::CheckPermissionRequest>(Check(Doc("dóc"))),
            $"invalid CheckPermissionRequest.Resource: {Embedded}invalid ObjectReference.ObjectId: {IdPattern}");

    [Fact]
    public void Object_id_is_at_most_1024_bytes() =>
        AssertRejected(
            Parse<V1::CheckPermissionRequest>(Check(Doc(new string('a', 1025)))),
            $"invalid CheckPermissionRequest.Resource: {Embedded}invalid ObjectReference.ObjectId: " +
            "value length must be at most 1024 bytes");

    [Fact]
    public void Object_type_over_128_bytes_reports_length_then_pattern() =>
        AssertRejected(
            Parse<V1::CheckPermissionRequest>(Check($$"""{"objectType":"{{new string('a', 129)}}","objectId":"x"}""")),
            $"invalid CheckPermissionRequest.Resource: {Embedded}" +
            "invalid ObjectReference.ObjectType: value length must be at most 128 bytes; " +
            $"invalid ObjectReference.ObjectType: {TypePattern}");

    [Fact]
    public void Permission_and_subject_relation_must_match_the_relation_grammar() =>
        AssertRejected(
            Parse<V1::CheckPermissionRequest>(Check(Doc("doc1"), permission: "View",
                subject: """{"object":{"objectType":"user","objectId":"alice"},"optionalRelation":"Member"}""")),
            $"invalid CheckPermissionRequest.Permission: {OptionalRelationPattern}; " +
            $"invalid CheckPermissionRequest.Subject: {Embedded}invalid SubjectReference.OptionalRelation: {OptionalRelationPattern}");

    [Fact]
    public void Permission_is_at_most_64_bytes() =>
        AssertRejected(
            Parse<V1::CheckPermissionRequest>(Check(Doc("doc1"), permission: new string('v', 65))),
            "invalid CheckPermissionRequest.Permission: value length must be at most 64 bytes; " +
            $"invalid CheckPermissionRequest.Permission: {OptionalRelationPattern}");

    [Fact]
    public void Missing_resource_and_subject_are_both_reported() =>
        AssertRejected(
            Parse<V1::CheckPermissionRequest>("""{"permission":"view"}"""),
            "invalid CheckPermissionRequest.Resource: value is required; " +
            "invalid CheckPermissionRequest.Subject: value is required");

    // SpiceDB's consistency middleware (pkg/middleware/consistency) runs before the protoc-gen-validate
    // interceptor: a Consistency with no usable requirement falls to its default case, INTERNAL
    // "missing handling of consistency case in %v", where %v is the message's compact text form.
    [Fact]
    public void An_empty_consistency_is_an_internal_error_before_field_validation() =>
        AssertRejected(
            Parse<V1::CheckPermissionRequest>(Check(Doc("Bad"), consistency: "\"consistency\":{},")),
            StatusCode.Internal, "missing handling of consistency case in ");

    [Fact]
    public void Minimize_latency_false_is_an_unhandled_consistency_case() =>
        AssertRejected(
            Parse<V1::CheckPermissionRequest>(Check(Doc("doc1"), consistency: "\"consistency\":{\"minimizeLatency\":false},")),
            StatusCode.Internal, "missing handling of consistency case in minimize_latency:false");

    [Fact]
    public void Fully_consistent_false_is_an_unhandled_consistency_case() =>
        AssertRejected(
            Parse<V1::ExpandPermissionTreeRequest>(
                """{"consistency":{"fullyConsistent":false},"resource":{"objectType":"document","objectId":"doc1"},"permission":"view"}"""),
            StatusCode.Internal, "missing handling of consistency case in fully_consistent:false");

    // The middleware decodes a ZedToken before field validation; an empty one never decodes.
    [Fact]
    public void An_empty_zed_token_is_an_invalid_revision() =>
        AssertRejected(
            Parse<V1::CheckPermissionRequest>(Check(Doc("doc 1"), consistency: "\"consistency\":{\"atLeastAsFresh\":{}},")),
            "invalid revision requested");

    [Fact]
    public void The_consistency_rules_still_hold_at_the_protoc_gen_validate_layer()
    {
        AssertPgv(
            Parse<V1::CheckPermissionRequest>(Check(Doc("doc1"), consistency: "\"consistency\":{},")),
            $"invalid CheckPermissionRequest.Consistency: {Embedded}invalid Consistency.Requirement: value is required");
        AssertPgv(
            Parse<V1::CheckPermissionRequest>(Check(Doc("doc1"), consistency: "\"consistency\":{\"minimizeLatency\":false},")),
            $"invalid CheckPermissionRequest.Consistency: {Embedded}invalid Consistency.MinimizeLatency: value must equal true");
        AssertPgv(
            Parse<V1::ExpandPermissionTreeRequest>(
                """{"consistency":{"fullyConsistent":false},"resource":{"objectType":"document","objectId":"doc1"},"permission":"view"}"""),
            $"invalid ExpandPermissionTreeRequest.Consistency: {Embedded}invalid Consistency.FullyConsistent: value must equal true");
        AssertPgv(
            Parse<V1::CheckPermissionRequest>(Check(Doc("doc1"), consistency: "\"consistency\":{\"atLeastAsFresh\":{}},")),
            $"invalid CheckPermissionRequest.Consistency: {Embedded}invalid Consistency.AtLeastAsFresh: {Embedded}" +
            "invalid ZedToken.Token: value length must be at least 1 bytes");
    }

    [Fact]
    public void A_check_on_a_wildcard_resource_is_rejected() =>
        AssertRejected(
            Parse<V1::CheckPermissionRequest>(Check(Doc("*"))),
            "invalid ObjectReference.ObjectId: alphanumeric value is required");

    [Fact]
    public void A_wildcard_subject_with_a_relation_is_rejected() =>
        AssertRejected(
            Parse<V1::CheckPermissionRequest>(Check(Doc("doc1"),
                subject: """{"object":{"objectType":"user","objectId":"*"},"optionalRelation":"member"}""")),
            "invalid SubjectReference.OptionalRelation: optionalrelation must be empty on subject if object ID is a wildcard");

    [Fact]
    public void Relationship_relation_must_match_the_relation_grammar() =>
        AssertRejected(
            Parse<V1::WriteRelationshipsRequest>($$"""{"updates":[{{Update(Rel(Doc("doc1"), relation: "Viewer"))}}]}"""),
            $"invalid WriteRelationshipsRequest.Updates[0]: {Embedded}invalid RelationshipUpdate.Relationship: {Embedded}" +
            $"invalid Relationship.Relation: {RelationPattern}");

    [Fact]
    public void Update_operation_must_be_a_defined_non_zero_value()
    {
        var unspecified = Update(Rel(Doc("doc1")), operation: "\"OPERATION_UNSPECIFIED\"");
        var undefined = Update(Rel(Doc("doc1"), subject: """{"object":{"objectType":"user","objectId":"bob"}}"""), operation: "99");
        AssertRejected(
            Parse<V1::WriteRelationshipsRequest>($$"""{"updates":[{{unspecified}},{{undefined}}]}"""),
            $"invalid WriteRelationshipsRequest.Updates[0]: {Embedded}" +
            "invalid RelationshipUpdate.Operation: value must not be in list [OPERATION_UNSPECIFIED]; " +
            $"invalid WriteRelationshipsRequest.Updates[1]: {Embedded}" +
            "invalid RelationshipUpdate.Operation: value must be one of the defined enum values");
    }

    [Fact]
    public void Caveat_name_must_match_its_grammar() =>
        AssertRejected(
            Parse<V1::WriteRelationshipsRequest>(
                $$"""{"updates":[{{Update(Rel(Doc("doc1"), extra: ",\"optionalCaveat\":{\"caveatName\":\"bad name\"}"))}}]}"""),
            $"invalid WriteRelationshipsRequest.Updates[0]: {Embedded}invalid RelationshipUpdate.Relationship: {Embedded}" +
            $"invalid Relationship.OptionalCaveat: {Embedded}invalid ContextualizedCaveat.CaveatName: {CaveatPattern}");

    [Fact]
    public void Writing_a_wildcard_resource_is_rejected() =>
        AssertRejected(
            Parse<V1::WriteRelationshipsRequest>($$"""{"updates":[{{Update(Rel(Doc("*")))}}]}"""),
            "invalid ObjectReference.ObjectId: alphanumeric value is required");

    [Fact]
    public void Writing_a_wildcard_subject_with_a_relation_is_rejected() =>
        AssertRejected(
            Parse<V1::WriteRelationshipsRequest>($$"""{"updates":[{{Update(Rel(Doc("doc1"),
                subject: """{"object":{"objectType":"user","objectId":"*"},"optionalRelation":"member"}"""))}}]}"""),
            "invalid SubjectReference.OptionalRelation: optionalrelation must be empty on subject if object ID is a wildcard");

    [Fact]
    public void A_precondition_filter_on_a_wildcard_resource_id_breaks_the_filter_id_grammar() =>
        AssertRejected(
            Parse<V1::WriteRelationshipsRequest>(
                """{"optionalPreconditions":[{"operation":"OPERATION_MUST_MATCH","filter":{"resourceType":"document","optionalResourceId":"*"}}]}"""),
            $"invalid WriteRelationshipsRequest.OptionalPreconditions[0]: {Embedded}invalid Precondition.Filter: {Embedded}" +
            $"invalid RelationshipFilter.OptionalResourceId: {FilterIdPattern}");

    [Fact]
    public void A_precondition_needs_an_operation_and_a_valid_filter() =>
        AssertRejected(
            Parse<V1::WriteRelationshipsRequest>("""{"optionalPreconditions":[{"filter":{"resourceType":"Document"}}]}"""),
            $"invalid WriteRelationshipsRequest.OptionalPreconditions[0]: {Embedded}" +
            "invalid Precondition.Operation: value must not be in list [OPERATION_UNSPECIFIED]; " +
            $"invalid Precondition.Filter: {Embedded}invalid RelationshipFilter.ResourceType: {OptionalTypePattern}");

    [Fact]
    public void A_wildcard_subject_filter_with_a_relation_is_rejected() =>
        AssertRejected(
            Parse<V1::DeleteRelationshipsRequest>(
                """{"relationshipFilter":{"resourceType":"document","optionalSubjectFilter":{"subjectType":"user","optionalSubjectId":"*","optionalRelation":{"relation":"member"}}}}"""),
            "invalid SubjectFilter.OptionalRelation: optionalrelation must be empty on subject if object ID is a wildcard");

    [Fact]
    public void Subject_filter_fields_must_match_their_grammar() =>
        AssertRejected(
            Parse<V1::DeleteRelationshipsRequest>(
                """{"relationshipFilter":{"resourceType":"document","optionalSubjectFilter":{"subjectType":"","optionalRelation":{"relation":"Member"}}}}"""),
            $"invalid DeleteRelationshipsRequest.RelationshipFilter: {Embedded}invalid RelationshipFilter.OptionalSubjectFilter: {Embedded}" +
            $"invalid SubjectFilter.SubjectType: {TypePattern}; " +
            $"invalid SubjectFilter.OptionalRelation: {Embedded}invalid SubjectFilter_RelationFilter.Relation: {OptionalRelationPattern}");

    [Fact]
    public void Every_relationship_filter_field_is_checked_in_order() =>
        AssertRejected(
            Parse<V1::ReadRelationshipsRequest>(
                """{"relationshipFilter":{"resourceType":"Document","optionalResourceId":"a b","optionalResourceIdPrefix":"c d","optionalRelation":"X"}}"""),
            $"invalid ReadRelationshipsRequest.RelationshipFilter: {Embedded}" +
            $"invalid RelationshipFilter.ResourceType: {OptionalTypePattern}; " +
            $"invalid RelationshipFilter.OptionalResourceId: {FilterIdPattern}; " +
            $"invalid RelationshipFilter.OptionalResourceIdPrefix: {FilterIdPattern}; " +
            $"invalid RelationshipFilter.OptionalRelation: {OptionalRelationPattern}");

    [Fact]
    public void A_cursor_token_must_be_between_1_and_102400_bytes()
    {
        const string reason = "value length must be between 1 and 102400 bytes, inclusive";
        AssertPgv(
            Parse<V1::ReadRelationshipsRequest>("""{"relationshipFilter":{"resourceType":"document"},"optionalCursor":{}}"""),
            $"invalid ReadRelationshipsRequest.OptionalCursor: {Embedded}invalid Cursor.Token: {reason}");
        AssertPgv(
            Parse<V1::ReadRelationshipsRequest>(
                $$$"""{"relationshipFilter":{"resourceType":"document"},"optionalCursor":{"token":"{{{new string('x', 102401)}}}"}}"""),
            $"invalid ReadRelationshipsRequest.OptionalCursor: {Embedded}invalid Cursor.Token: {reason}");
    }

    // With a cursor present the middleware takes the revision from it, ahead of field validation. An empty
    // token decodes to a cursor with no version, which it reports as an unexpected (INTERNAL) error.
    [Fact]
    public void An_empty_cursor_is_an_internal_error_before_field_validation()
    {
        const string detail = "unexpected consistency middleware error: cursor pointer was nil";
        AssertRejected(
            Parse<V1::ReadRelationshipsRequest>("""{"relationshipFilter":{"resourceType":"Document"},"optionalCursor":{},"consistency":{}}"""),
            StatusCode.Internal, detail);
        AssertRejected(
            Parse<V1::ExportBulkRelationshipsRequest>("""{"optionalRelationshipFilter":{"resourceType":"Document"},"optionalCursor":{}}"""),
            StatusCode.Internal, detail);
    }

    [Fact]
    public void Lookup_resources_checks_resource_type_and_requires_a_subject()
    {
        AssertRejected(
            Parse<V1::LookupResourcesRequest>($$"""{"resourceObjectType":"Document","permission":"view","subject":{{Alice}}}"""),
            $"invalid LookupResourcesRequest.ResourceObjectType: {TypePattern}");
        AssertRejected(
            Parse<V1::LookupResourcesRequest>("""{"resourceObjectType":"document","permission":"view"}"""),
            "invalid LookupResourcesRequest.Subject: value is required");
    }

    [Fact]
    public void Lookup_subjects_checks_subject_type_and_relation() =>
        AssertRejected(
            Parse<V1::LookupSubjectsRequest>(
                """{"resource":{"objectType":"document","objectId":"doc1"},"permission":"view","subjectObjectType":"User","optionalSubjectRelation":"Member"}"""),
            $"invalid LookupSubjectsRequest.SubjectObjectType: {TypePattern}; " +
            $"invalid LookupSubjectsRequest.OptionalSubjectRelation: {OptionalRelationPattern}");

    [Fact]
    public void Expand_rejects_a_wildcard_resource() =>
        AssertRejected(
            Parse<V1::ExpandPermissionTreeRequest>("""{"resource":{"objectType":"document","objectId":"*"},"permission":"view"}"""),
            "invalid ObjectReference.ObjectId: alphanumeric value is required");

    [Fact]
    public void Check_bulk_reports_each_item_by_index() =>
        AssertRejected(
            Parse<V1::CheckBulkPermissionsRequest>(
                $$"""{"items":[{{Check(Doc("doc1"))}},{"resource":{"objectType":"Document","objectId":"doc1"},"permission":"view"}]}"""),
            $"invalid CheckBulkPermissionsRequest.Items[1]: {Embedded}" +
            $"invalid CheckBulkPermissionsRequestItem.Resource: {Embedded}invalid ObjectReference.ObjectType: {TypePattern}; " +
            "invalid CheckBulkPermissionsRequestItem.Subject: value is required");

    [Fact]
    public void Import_bulk_reports_each_relationship_by_index() =>
        AssertRejected(
            Parse<V1::ImportBulkRelationshipsRequest>(
                $$"""{"relationships":[{{Rel(Doc("doc1"))}},{"resource":{"objectType":"document","objectId":"doc 2"},"relation":"viewer"}]}"""),
            $"invalid ImportBulkRelationshipsRequest.Relationships[1]: {Embedded}" +
            $"invalid Relationship.Resource: {Embedded}invalid ObjectReference.ObjectId: {IdPattern}; " +
            "invalid Relationship.Subject: value is required");

    [Fact]
    public void Export_bulk_checks_fields_in_declaration_order() =>
        AssertPgv(
            Parse<V1::ExportBulkRelationshipsRequest>("""{"optionalRelationshipFilter":{"resourceType":"Document"},"optionalCursor":{}}"""),
            $"invalid ExportBulkRelationshipsRequest.OptionalCursor: {Embedded}" +
            "invalid Cursor.Token: value length must be between 1 and 102400 bytes, inclusive; " +
            $"invalid ExportBulkRelationshipsRequest.OptionalRelationshipFilter: {Embedded}" +
            $"invalid RelationshipFilter.ResourceType: {OptionalTypePattern}");

    [Fact]
    public void Counter_names_must_match_the_relation_grammar()
    {
        AssertRejected(
            Parse<V1::ExperimentalRegisterRelationshipCounterRequest>("""{"name":"Bad"}"""),
            $"invalid ExperimentalRegisterRelationshipCounterRequest.Name: {OptionalRelationPattern}; " +
            "invalid ExperimentalRegisterRelationshipCounterRequest.RelationshipFilter: value is required");
        AssertRejected(
            Parse<V1::ExperimentalCountRelationshipsRequest>("""{"name":"Bad"}"""),
            $"invalid ExperimentalCountRelationshipsRequest.Name: {OptionalRelationPattern}");
        AssertRejected(
            Parse<V1::ExperimentalUnregisterRelationshipCounterRequest>("""{"name":"Bad"}"""),
            $"invalid ExperimentalUnregisterRelationshipCounterRequest.Name: {OptionalRelationPattern}");
    }

    [Fact]
    public void Schema_requests_validate_their_consistency_and_size()
    {
        // The schema service's requests pass through the same consistency middleware.
        AssertRejected(
            Parse<V1::ReflectSchemaRequest>("""{"consistency":{}}"""),
            StatusCode.Internal, "missing handling of consistency case in ");
        AssertRejected(
            Parse<V1::DiffSchemaRequest>("""{"comparisonSchema":"x","consistency":{}}"""),
            StatusCode.Internal, "missing handling of consistency case in ");
        AssertRejected(
            new V1::WriteSchemaRequest { Schema = new string('x', 4_194_305) },
            "invalid WriteSchemaRequest.Schema: value length must be at most 4194304 bytes");
    }

    /// <summary>
    /// Every rule on every message an <c>authzed.api.v1</c> service receives is one the validator implements,
    /// so a re-vendored proto that adds a new kind of rule fails here rather than going unenforced.
    /// </summary>
    [Fact]
    public void Every_request_message_has_a_supported_plan()
    {
        var services = new[]
        {
            V1::PermissionServiceReflection.Descriptor,
            V1::SchemaServiceReflection.Descriptor,
            V1::WatchServiceReflection.Descriptor,
            V1::ExperimentalServiceReflection.Descriptor,
        };
        foreach (MethodDescriptor method in services.SelectMany(f => f.Services).SelectMany(s => s.Methods))
            ProtoValidation.EnsureSupported(method.InputType);
    }

    [Theory]
    [InlineData("object_id", "ObjectId")]
    [InlineData("optional_resource_id_prefix", "OptionalResourceIdPrefix")]
    [InlineData("requirement", "Requirement")]
    [InlineData("a_1", "A_1")]
    public void Go_names_follow_protoc_gen_go(string proto, string go) => Assert.Equal(go, ProtoValidation.GoName(proto));

    [Theory]
    [InlineData("^a$", "a", true)]
    [InlineData("^a$", "a\n", false)]
    [InlineData("^[$]$", "$", true)]
    public void Go_patterns_anchor_at_the_very_end(string pattern, string input, bool matches) =>
        Assert.Equal(matches, ProtoValidation.GoRegex(pattern).IsMatch(input));
}
