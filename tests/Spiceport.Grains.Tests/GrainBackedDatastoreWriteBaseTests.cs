using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Gates for <see cref="GrainBackedDatastore.ReadWriteTx"/>'s COMPATIBILITY write path: each attempt
/// derives its write base (and its <c>ExpectedHead</c> CAS) from ONE per-attempt
/// <see cref="IDatastoreGrain.ReadState"/> fetch — the documented per-write full-state cost this path
/// accepts because production writes are declarative through <see cref="IDatastoreGrain.Commit"/> and only
/// tests/BulkImport/SeedData drive this lambda shape. Driven against a hand-rolled in-process
/// <see cref="IDatastoreGrain"/> fake (no Orleans) that counts every grain-interface call, reusing the same
/// <see cref="LogFold"/> the real <c>DatastoreGrain</c> folds through, so the fake's CAS/fold semantics are
/// provably the same shape as production.
/// </summary>
public sealed class GrainBackedDatastoreWriteBaseTests
{
    private static Relationship Rel(string rid, string sid) =>
        Relationship.Create(new ObjectAndRelation("doc", rid, "viewer"), new ObjectAndRelation("user", sid, CoreConstants.Ellipsis));

    private static (GrainBackedDatastore Datastore, CountingFakeGrain Grain) NewDatastore()
    {
        var grain = new CountingFakeGrain();
        var factory = new SingleGrainFactory(grain);
        // A real hub instance (never started — these tests never call Watch); Pulse on commit is a pure
        // in-memory signal, so no observer registration or heartbeat is involved.
        var ds = new GrainBackedDatastore(factory, new LogWatchHub(grain, factory));
        return (ds, grain);
    }

    [Fact]
    public async Task SequentialWrites_PerformExactlyOneReadStatePerAttempt()
    {
        var (ds, grain) = NewDatastore();

        for (var i = 0; i < 5; i++)
        {
            var id = i.ToString();
            await ds.ReadWriteTx(tx => tx.WriteRelationships(
                new[] { new RelationshipUpdate(Rel(id, "alice"), UpdateOperation.Create) }));
        }

        // One base fetch per (non-retried) attempt, and nothing else: no separate head probe (the base
        // state's own HeadRevision is the CAS ExpectedHead, keeping base and CAS exactly in agreement).
        Assert.Equal(5, grain.ReadStateCalls);
        Assert.Equal(0, grain.GetHeadCalls);
        Assert.Equal(5, grain.CommitCalls);
    }

    [Fact]
    public async Task RacingWrites_LoserRetries_FromAFreshBase_AndBothLand()
    {
        var (ds, grain) = NewDatastore();

        // Force a genuine race: both writers' FIRST Commit rendezvous here (a 2-party barrier), so
        // both requests were built from bases that predate EITHER commit — guaranteeing exactly one loses
        // the CAS. A retry's later Commit arrives after the barrier has opened, so it passes straight
        // through.
        var arrivals = 0;
        var bothArrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        grain.OnCommit = () =>
        {
            if (Interlocked.Increment(ref arrivals) >= 2)
                bothArrived.TrySetResult();
            return bothArrived.Task;
        };

        var t1 = ds.ReadWriteTx(tx => tx.WriteRelationships(
            new[] { new RelationshipUpdate(Rel("a", "alice"), UpdateOperation.Create) }));
        var t2 = ds.ReadWriteTx(tx => tx.WriteRelationships(
            new[] { new RelationshipUpdate(Rel("b", "bob"), UpdateOperation.Create) }));
        await Task.WhenAll(t1, t2);

        // Exactly one Commit attempt lost the CAS and retried (2 that raced + 1 retry); the retry
        // re-fetched a FRESH base (2 first attempts + 1 retry = 3 ReadState calls), so its lambda
        // re-evaluated against the winner's committed state — race-free by construction.
        Assert.Equal(3, grain.CommitCalls);
        Assert.Equal(3, grain.ReadStateCalls);

        var head = await ds.HeadRevision();
        var reader = ds.SnapshotReader(head.Revision);
        var live = new HashSet<string>();
        await foreach (var rel in reader.QueryRelationships(new RelationshipsFilter()))
            live.Add(rel.Resource.ObjectId);
        Assert.Equal(new HashSet<string> { "a", "b" }, live);
    }

    /// <summary>
    /// Read-your-writes across sequential <see cref="GrainBackedDatastore.ReadWriteTx"/> calls on the same
    /// instance: the second write's precondition (create-conflict on an already-live key) must see the first
    /// write's row through its per-attempt base fetch (the grain confirms the new head before Commit
    /// returns, so the next attempt's ReadState observes it). This is the same property
    /// <c>GrainBackedDatastoreFidelityTests.GrainBackedDatastore_MatchesReferenceDatastore_AtLiveSetLevel</c>
    /// (steps 2 and 4) already proves end-to-end against the real grain; this test isolates it against the
    /// fake so it fails loudly if the write base ever goes stale.
    /// </summary>
    [Fact]
    public async Task SecondWrite_SeesFirstWrites_CreateConflict()
    {
        var (ds, _) = NewDatastore();
        var a = Rel("a", "alice");

        await ds.ReadWriteTx(tx => tx.WriteRelationships(new[] { new RelationshipUpdate(a, UpdateOperation.Create) }));

        await Assert.ThrowsAsync<CreateRelationshipExistsException>(() =>
            ds.ReadWriteTx(tx => tx.WriteRelationships(new[] { new RelationshipUpdate(a, UpdateOperation.Create) })));
    }

    /// <summary>An <see cref="IGrainFactory"/> that hands back one fixed grain instance for every lookup.</summary>
    private sealed class SingleGrainFactory(IDatastoreGrain grain) : IGrainFactory
    {
        private readonly IDatastoreGrain _grain = grain;

        public TGrainInterface GetGrain<TGrainInterface>(Guid primaryKey, string? grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithGuidKey => throw new NotSupportedException();

        public TGrainInterface GetGrain<TGrainInterface>(long primaryKey, string? grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithIntegerKey =>
            _grain is TGrainInterface t ? t : throw new NotSupportedException();

        public TGrainInterface GetGrain<TGrainInterface>(string primaryKey, string? grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithStringKey => throw new NotSupportedException();

        public TGrainInterface GetGrain<TGrainInterface>(Guid primaryKey, string keyExtension, string? grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithGuidCompoundKey => throw new NotSupportedException();

        public TGrainInterface GetGrain<TGrainInterface>(long primaryKey, string keyExtension, string? grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithIntegerCompoundKey => throw new NotSupportedException();

        public TGrainObserverInterface CreateObjectReference<TGrainObserverInterface>(IGrainObserver obj)
            where TGrainObserverInterface : IGrainObserver => throw new NotSupportedException();

        public void DeleteObjectReference<TGrainObserverInterface>(IGrainObserver obj)
            where TGrainObserverInterface : IGrainObserver => throw new NotSupportedException();

        public IGrain GetGrain(Type grainInterfaceType, Guid grainPrimaryKey) => throw new NotSupportedException();
        public IGrain GetGrain(Type grainInterfaceType, long grainPrimaryKey) => throw new NotSupportedException();
        public IGrain GetGrain(Type grainInterfaceType, string grainPrimaryKey) => throw new NotSupportedException();
        public IGrain GetGrain(Type grainInterfaceType, Guid grainPrimaryKey, string keyExtension) => throw new NotSupportedException();
        public IGrain GetGrain(Type grainInterfaceType, long grainPrimaryKey, string keyExtension) => throw new NotSupportedException();
        public TGrainInterface GetGrain<TGrainInterface>(GrainId grainId) where TGrainInterface : IAddressable => throw new NotSupportedException();
        public IAddressable GetGrain(GrainId grainId) => throw new NotSupportedException();
        public IAddressable GetGrain(GrainId grainId, GrainInterfaceType interfaceType) => throw new NotSupportedException();
        public IAddressable GetGrain(Type interfaceType, IdSpan grainKey, string grainClassNamePrefix) => throw new NotSupportedException();
        public IAddressable GetGrain(Type interfaceType, IdSpan grainKey) => throw new NotSupportedException();
    }

    /// <summary>
    /// A minimal in-process <see cref="IDatastoreGrain"/> that owns the append-only log and counts every
    /// interface call, so the write-path tests can assert exactly which grain calls happen. Reuses the real
    /// <see cref="LogFold"/> (proposal-to-event and fold) so its CAS/state semantics track the production
    /// <c>DatastoreGrain</c>. Serialized under a lock, mirroring the grain's own single-activation semantics.
    /// </summary>
    private sealed class CountingFakeGrain : IDatastoreGrain
    {
        private readonly object _lock = new();
        private DatastoreGrainState _state = DatastoreGrainState.Empty(0);
        private readonly List<LogEvent> _events = new();

        public int ReadStateCalls { get; private set; }
        public int GetHeadCalls { get; private set; }
        public int CommitCalls { get; private set; }
        public int ReadFromCalls { get; private set; }

        public Task<DatastoreGrainState> ReadState()
        {
            lock (_lock)
            {
                ReadStateCalls++;
                return Task.FromResult(_state);
            }
        }

        /// <summary>
        /// Test-only rendezvous hook, awaited before every <see cref="Commit"/> call takes the lock.
        /// Used by <c>RacingWrites_LoserRetries_FromAFreshBase_AndBothLand</c> to force two
        /// concurrent <see cref="GrainBackedDatastore.ReadWriteTx"/> calls to genuinely race — both
        /// requests built from pre-commit bases — rather than one running this synchronous in-process fake
        /// to completion before the other starts. Null (the default) is a no-op.
        /// </summary>
        public Func<Task>? OnCommit;

        public Task<DatastoreHeadWire> GetHead()
        {
            lock (_lock)
            {
                GetHeadCalls++;
                return Task.FromResult(
                    new DatastoreHeadWire(_state.HeadRevision, _state.SchemaHashAt(_state.HeadRevision), _state.GcFloor));
            }
        }

        /// <summary>
        /// The compatibility-path slice of the real grain's Commit: the ExpectedHead CAS plus the
        /// LogFold append. Sufficient here because <see cref="GrainBackedDatastore.ReadWriteTx"/> only
        /// ever sends the already-resolved net diff (no preconditions, no delete-by-filter, always an
        /// ExpectedHead), with every guarded operation evaluated client-side by the lambda.
        /// </summary>
        public async Task<CommitReply> Commit(CommitRequest request)
        {
            if (OnCommit is { } hook)
                await hook();
            lock (_lock)
            {
                CommitCalls++;
                if (request.ExpectedHead is { } expectedHead && _state.HeadRevision != expectedHead)
                    return new CommitReply(
                        null, new CommitFailureWire(CommitFailureKind.HeadMoved, "head moved"), 0, false);

                var now = (DateTimeOffset.UtcNow - DateTimeOffset.UnixEpoch).Ticks * 100L;
                var head = _state.HeadRevision;
                var newRevision = now > head ? now : head + 1;
                var ev = LogFold.EventFromProposal(
                    new ProposedWrite(request.Updates, request.SchemaBytes, request.CounterChanges), newRevision);
                _events.Add(ev);
                _state = LogFold.ApplyEvent(_state, ev);
                return new CommitReply(newRevision, null, 0, false);
            }
        }

        public Task<LogSegment> ReadFrom(long afterRevision, int maxCount)
        {
            lock (_lock)
            {
                ReadFromCalls++;
                var head = _state.HeadRevision;
                var page = _events
                    .Where(e => e.Revision > afterRevision)
                    .OrderBy(e => e.Revision)
                    .Take(maxCount < 0 ? int.MaxValue : maxCount)
                    .ToList();
                return Task.FromResult(new LogSegment(page, head));
            }
        }

        public Task<DatastoreHeadWire> SubscribeWatch(IDatastoreWatcher watcher) => GetHead();
        public Task UnsubscribeWatch(IDatastoreWatcher watcher) => Task.CompletedTask;
        public Task<long?> RunGc() => Task.FromResult<long?>(null);

        // The write path under test never hydrates shards; a call here would be a routing bug.
        public Task<GraphShardState> ReadShard(GraphShardKeyWire key) => throw new NotSupportedException();

        public Task<byte[]?> ReadSchemaAt(long revision)
        {
            lock (_lock)
            {
                return Task.FromResult(_state.SchemaVersionAt(revision)?.Bytes);
            }
        }
    }
}
