using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Spiceport.Api;
using Spiceport.Grains;
using Spiceport.Schema;
using V1 = Authzed.Api.V1;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Pure tests (no cluster) for the <c>google.rpc.ErrorInfo</c> SpiceDB attaches to the errors it gives a
/// reason (<c>pkg/spiceerrors.ForReason</c>): domain <c>authzed.com</c>, the <c>ErrorReason</c> name, and
/// the metadata each error type's <c>GRPCStatus</c> builds, carried in the <c>grpc-status-details-bin</c>
/// trailer. Each expectation names the SpiceDB v1.49.2 error type it mirrors.
/// </summary>
public class ErrorInfoTests
{
    internal static void AssertErrorInfo(
        RpcException ex, StatusCode code, string reason, params (string Key, string Value)[] metadata)
    {
        Assert.Equal(code, ex.StatusCode);
        var status = RpcErrors.ToRpcStatus(ex);
        Assert.Equal((int)code, status.Code);
        Assert.Equal(ex.Status.Detail, status.Message);

        var info = RpcErrors.ErrorInfoOf(ex);
        Assert.NotNull(info);
        Assert.Equal("authzed.com", info.Domain);
        Assert.Equal(reason, info.Reason);
        Assert.Equal(
            metadata.OrderBy(m => m.Key, StringComparer.Ordinal).ToList(),
            info.Metadata.Select(m => (m.Key, m.Value)).OrderBy(m => m.Key, StringComparer.Ordinal).ToList());
    }

    private static V1::Relationship Rel(string resourceId, string relation = "viewer") => new()
    {
        Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = resourceId },
        Relation = relation,
        Subject = new V1::SubjectReference { Object = new V1::ObjectReference { ObjectType = "user", ObjectId = "alice" } },
    };

    private static V1::RelationshipUpdate Touch(V1::Relationship rel) =>
        new() { Operation = V1::RelationshipUpdate.Types.Operation.Touch, Relationship = rel };

    [Fact]
    public void Reason_details_travel_in_the_status_details_trailer()
    {
        var ex = RpcErrors.WithReason(
            StatusCode.InvalidArgument, "boom", V1::ErrorReason.InvalidCursor, [new("k", "v")]);

        var trailer = ex.Trailers.GetValueBytes("grpc-status-details-bin");
        Assert.NotNull(trailer);
        var status = Google.Rpc.Status.Parser.ParseFrom(trailer);
        Assert.Equal((int)StatusCode.InvalidArgument, status.Code);
        Assert.Equal("boom", status.Message);
        var info = Assert.Single(status.Details).Unpack<Google.Rpc.ErrorInfo>();
        Assert.Equal("ERROR_REASON_INVALID_CURSOR", info.Reason);
        Assert.Equal("authzed.com", info.Domain);
        Assert.Equal("v", info.Metadata["k"]);
    }

    [Fact]
    public void A_status_without_details_converts_to_a_bare_rpc_status()
    {
        var status = RpcErrors.ToRpcStatus(new RpcException(new Status(StatusCode.NotFound, "gone")));
        Assert.Equal((int)StatusCode.NotFound, status.Code);
        Assert.Equal("gone", status.Message);
        Assert.Empty(status.Details);
    }

    // ExceedsMaximumLimitError
    [Fact]
    public void An_over_limit_page_size_exceeds_the_maximum_allowable_limit() =>
        AssertErrorInfo(
            Assert.Throws<RpcException>(() => RequestCeilings.ValidateLimit(5000, 1000)),
            StatusCode.InvalidArgument, "ERROR_REASON_EXCEEDS_MAXIMUM_ALLOWABLE_LIMIT",
            ("limit_provided", "5000"), ("maximum_limit_allowed", "1000"));

    // TransactionMetadataTooLargeError
    [Fact]
    public void Oversized_transaction_metadata_is_too_large()
    {
        var metadata = new Struct { Fields = { ["k"] = Value.ForString(new string('x', 65_000)) } };
        var size = System.Text.Encoding.UTF8.GetByteCount(Google.Protobuf.JsonFormatter.Default.Format(metadata));
        AssertErrorInfo(
            Assert.Throws<RpcException>(() => RequestCeilings.ValidateTransactionMetadata(metadata)),
            StatusCode.InvalidArgument, "ERROR_REASON_TRANSACTION_METADATA_TOO_LARGE",
            ("metadata_byte_size", size.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            ("maximum_allowed_metadata_byte_size", "65000"));
    }

    // ExceedsMaximumUpdatesError
    [Fact]
    public void Too_many_updates_in_a_write()
    {
        var request = new V1::WriteRelationshipsRequest();
        for (var i = 0; i < 1001; i++)
            request.Updates.Add(Touch(Rel($"doc{i}")));
        AssertErrorInfo(
            Assert.Throws<RpcException>(() => RequestLimits.ValidateWriteRelationships(request)),
            StatusCode.InvalidArgument, "ERROR_REASON_TOO_MANY_UPDATES_IN_REQUEST",
            ("update_count", "1001"), ("maximum_updates_allowed", "1000"));
    }

    // ExceedsMaximumPreconditionsError (its metadata reuses the key maximum_updates_allowed)
    [Fact]
    public void Too_many_preconditions_in_a_write()
    {
        var request = new V1::WriteRelationshipsRequest();
        for (var i = 0; i < 1001; i++)
        {
            request.OptionalPreconditions.Add(new V1::Precondition
            {
                Operation = V1::Precondition.Types.Operation.MustMatch,
                Filter = new V1::RelationshipFilter { ResourceType = "document" },
            });
        }
        AssertErrorInfo(
            Assert.Throws<RpcException>(() => RequestLimits.ValidateWriteRelationships(request)),
            StatusCode.InvalidArgument, "ERROR_REASON_TOO_MANY_PRECONDITIONS_IN_REQUEST",
            ("precondition_count", "1001"), ("maximum_updates_allowed", "1000"));
    }

    // DuplicateRelationErrorshipError: the relationship is MustV1StringRelationship, caveat and expiration
    // included.
    [Fact]
    public void A_relationship_updated_twice_is_updates_on_same_relationship()
    {
        var caveated = Rel("doc1");
        caveated.OptionalCaveat = new V1::ContextualizedCaveat
        {
            CaveatName = "only_on_tuesday",
            Context = new Struct { Fields = { ["day"] = Value.ForString("tuesday"), ["count"] = Value.ForNumber(2) } },
        };
        caveated.OptionalExpiresAt = Timestamp.FromDateTimeOffset(new DateTimeOffset(2030, 1, 2, 3, 4, 5, 500, TimeSpan.Zero));
        var request = new V1::WriteRelationshipsRequest { Updates = { Touch(Rel("doc1")), Touch(caveated) } };

        AssertErrorInfo(
            Assert.Throws<RpcException>(() => RequestLimits.ValidateWriteRelationships(request)),
            StatusCode.InvalidArgument, "ERROR_REASON_UPDATES_ON_SAME_RELATIONSHIP",
            ("definition_name", "document"),
            ("relationship",
                """document:doc1#viewer@user:alice[only_on_tuesday:{"count":2,"day":"tuesday"}][expiration:2030-01-02T03:04:05.5Z]"""));
    }

    // ErrMaxRelationshipContextError: context_size is proto.Size of the whole relationship.
    [Fact]
    public void An_oversized_relationship_caveat_context_exceeds_the_maximum()
    {
        var rel = Rel("doc1");
        rel.OptionalCaveat = new V1::ContextualizedCaveat
        {
            CaveatName = "c",
            Context = new Struct { Fields = { ["k"] = Value.ForString(new string('x', 25_000)) } },
        };
        var request = new V1::WriteRelationshipsRequest { Updates = { Touch(rel) } };

        AssertErrorInfo(
            Assert.Throws<RpcException>(() => RequestLimits.ValidateWriteRelationships(request)),
            StatusCode.InvalidArgument, "ERROR_REASON_MAX_RELATIONSHIP_CONTEXT_SIZE",
            ("relationship", "document:doc1#viewer@user:alice"),
            ("max_allowed_size", "25000"),
            ("context_size", rel.CalculateSize().ToString(System.Globalization.CultureInfo.InvariantCulture)));
    }

    private static SchemaSnapshot Snapshot(string text) => new(SchemaCompiler.CompileSchema(text), "h", text, 0);

    private const string Schema = """
        definition user {}

        definition document {
            relation viewer: user
            permission view = viewer
        }
        """;

    // NamespaceNotFoundError through rewriteError
    [Fact]
    public void An_unknown_definition_is_unknown_definition() =>
        AssertErrorInfo(
            Assert.Throws<RpcException>(() => SchemaValidation.CheckNamespaceAndRelations(
                Snapshot(Schema), new SchemaValidation.TypeAndRelation("folder", "view", AllowEllipsis: false))),
            StatusCode.FailedPrecondition, "ERROR_REASON_UNKNOWN_DEFINITION",
            ("definition_name", "folder"));

    // RelationNotFoundError through rewriteError
    [Fact]
    public void An_unknown_relation_is_unknown_relation_or_permission() =>
        AssertErrorInfo(
            Assert.Throws<RpcException>(() => SchemaValidation.CheckNamespaceAndRelations(
                Snapshot(Schema), new SchemaValidation.TypeAndRelation("document", "edit", AllowEllipsis: false))),
            StatusCode.FailedPrecondition, "ERROR_REASON_UNKNOWN_RELATION_OR_PERMISSION",
            ("definition_name", "document"), ("relation_or_permission_name", "edit"));

    // WildcardNotAllowedError from checkInternal
    [Fact]
    public void Checking_a_wildcard_subject_is_wildcard_not_allowed() =>
        AssertErrorInfo(
            Assert.Throws<RpcException>(() => SchemaValidation.RejectWildcardSubject("*")),
            StatusCode.InvalidArgument, "ERROR_REASON_WILDCARD_NOT_ALLOWED",
            ("field", "subject.object_id"));

    // A bulk-check pair's error is status.Convert(err).Proto(): its details come along.
    [Fact]
    public void A_bulk_check_pair_error_keeps_its_details()
    {
        var status = SchemaValidation.ToRpcStatus(SchemaValidation.WildcardSubjectError("*")!);
        Assert.Equal((int)StatusCode.InvalidArgument, status.Code);
        var info = Assert.Single(status.Details).Unpack<Google.Rpc.ErrorInfo>();
        Assert.Equal("ERROR_REASON_WILDCARD_NOT_ALLOWED", info.Reason);
    }

    // pkg/cursor InvalidCursorError (no metadata), for an undecodable cursor and one minted for another call
    [Fact]
    public void An_undecodable_or_foreign_cursor_is_an_invalid_cursor()
    {
        AssertErrorInfo(
            Assert.Throws<RpcException>(() => ReadRelationshipsCursor.Decode(new V1::Cursor { Token = "%%%" }, "hash")),
            StatusCode.InvalidArgument, "ERROR_REASON_INVALID_CURSOR");
        var foreign = ReadRelationshipsCursor.Encode("other", "document:doc1#viewer@user:alice");
        AssertErrorInfo(
            Assert.Throws<RpcException>(() => ReadRelationshipsCursor.Decode(new V1::Cursor { Token = foreign }, "hash")),
            StatusCode.InvalidArgument, "ERROR_REASON_INVALID_CURSOR");
    }

    // v1 InvalidCursorError: NewInvalidCursorErr never sets the reason field, so its metadata carries an
    // empty "reason".
    [Fact]
    public void An_unparseable_resume_relationship_is_an_invalid_cursor_with_an_empty_reason()
    {
        var cursor = ReadRelationshipsCursor.Encode("hash", "not a tuple");
        AssertErrorInfo(
            Assert.Throws<RpcException>(() => ReadRelationshipsCursor.Decode(new V1::Cursor { Token = cursor }, "hash")),
            StatusCode.FailedPrecondition, "ERROR_REASON_INVALID_CURSOR", ("reason", ""));
    }

    /// <summary>Go protojson's rendering of each number (from protobuf-go v1.36.10's encoder).</summary>
    [Theory]
    [InlineData(2d, "2")]
    [InlineData(0.5, "0.5")]
    [InlineData(1e21, "1e+21")]
    [InlineData(1.5e21, "1.5e+21")]
    [InlineData(1e-7, "1e-7")]
    [InlineData(1.25e-7, "1.25e-7")]
    [InlineData(123456789012d, "123456789012")]
    [InlineData(1e20, "100000000000000000000")]
    [InlineData(0.000001, "0.000001")]
    [InlineData(-3.75, "-3.75")]
    [InlineData(1e300, "1e+300")]
    [InlineData(5e-324, "5e-324")]
    [InlineData(0.1, "0.1")]
    public void Caveat_context_numbers_render_as_go_protojson_does(double value, string expected) =>
        Assert.Equal(expected, GoRelationshipStrings.GoFloat(value));
}
