using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Storage;
using Orleans.TestingHost;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests.FaultInjection;

/// <summary>One harness run: <paramref name="Seed"/> fixes every choice, <paramref name="Steps"/> its length.</summary>
public sealed record FaultHarnessOptions(int Seed, int Steps)
{
    /// <summary>Plants a lost-write bug in storage, for the harness's own gate.</summary>
    public bool LoseShardWrites { get; init; }

    /// <summary>The number of silos in the cluster.</summary>
    public int Silos { get; init; } = 1;

    /// <summary>
    /// Hard-kills secondary silos at seeded points, always leaving the primary and one secondary. Killed
    /// silos are not replaced: an in-process silo started on a killed one's address finds the zombie
    /// still in membership, and Orleans treats that as fatal for the whole process.
    /// </summary>
    public bool KillSilos { get; init; }

    /// <summary>When set, runs GC rounds at seeded points under this retention window.</summary>
    public TimeSpan? GcWindow { get; init; }
}

/// <summary>
/// A completed run: the trace (one line per step and per commit outcome, independent of timing), how
/// many injected write failures actually fired (as opposed to being armed), how many silos were killed
/// and how many of those held the sequencer's activation, and how many GC rounds advanced the floor.
/// </summary>
public sealed record FaultHarnessRun(
    IReadOnlyList<string> Trace,
    int SequencerFaultsFired,
    int ShardFaultsFired,
    int SilosKilled,
    int SequencerKills,
    int GcRounds);

/// <summary>The real system and the reference disagreed. The message names the seed and the trace.</summary>
public sealed class FaultHarnessDivergenceException(string message) : Exception(message);

/// <summary>
/// The deterministic fault-injection harness. A seeded workload of declarative commits (touches, creates,
/// deletes, preconditions, delete-by-filter) runs against the real sequencer and shard mesh in a
/// <see cref="TestCluster"/>, sequentially and in concurrent groups, with sequencer and shard-projection
/// write failures, explicit projection flushes and whole-cluster deactivations injected at seeded points,
/// and optionally hard silo kills and GC rounds. Storage is <see cref="SharedMemoryGrainStorage"/>, which
/// survives a killed silo as a database would.
/// Every commit is replayed against <see cref="ReferenceDatastore"/> in the order the sequencer assigned
/// and must reach the same outcome; reads through the shard mesh at every committed revision must
/// return what the reference returns at the matching revision.
/// </summary>
/// <remarks>
/// The seed fixes the workload and the fault schedule, not the silo's thread interleaving: concurrent
/// commits in one group touch disjoint resources, so every outcome is the same whatever order the
/// sequencer gives them, and the trace (which records outcomes, never revisions or timings) reproduces
/// exactly. The reference is replayed in the sequencer's order, so the state checks hold for any order.
/// Kills and GC rounds happen between steps, never under an in-flight call, so no commit's outcome is
/// ever unknown. Reads compare only at revisions at or above the GC floor.
/// </remarks>
public static class FaultHarness
{
    private const string Schema = "definition user {}\n\ndefinition doc {\n    relation viewer: user\n}";
    private static readonly string[] Docs = ["d0", "d1", "d2", "d3", "d4", "d5"];
    private static readonly string[] Users = ["u0", "u1", "u2", "u3", "u4"];
    private static readonly TimeSpan CallTimeout = TimeSpan.FromSeconds(30);

    public static async Task<FaultHarnessRun> RunAsync(FaultHarnessOptions options)
    {
        StorageFaults.Reset();
        SharedMemoryGrainStorage.Reset();
        StorageFaults.LoseShardWrites = options.LoseShardWrites;
        HarnessSiloConfigurator.GcWindow = options.GcWindow;
        var builder = new TestClusterBuilder(initialSilosCount: (short)options.Silos);
        builder.AddSiloBuilderConfigurator<HarnessSiloConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        try
        {
            var run = new Run(options, cluster);
            await run.Execute();
            return new FaultHarnessRun(
                run.Trace, StorageFaults.InjectedSequencerFaults, StorageFaults.InjectedShardFaults,
                run.SilosKilled, run.SequencerKills, run.GcRounds);
        }
        finally
        {
            StorageFaults.Reset();
            await cluster.DisposeAsync();
            SharedMemoryGrainStorage.Reset();
        }
    }

    private sealed class Run(FaultHarnessOptions options, TestCluster cluster)
    {
        private readonly Random _rng = new(options.Seed);
        private readonly ReferenceDatastore _reference = new();
        private readonly List<long> _revisions = [];
        private readonly Dictionary<long, IRevision> _referenceAt = [];
        private long _gcFloor;

        public List<string> Trace { get; } = [];

        public int SilosKilled { get; private set; }

        public int SequencerKills { get; private set; }

        public int GcRounds { get; private set; }

        // Every call goes from inside the primary silo, which is never killed, as production callers do:
        // the external test client keeps a killed silo in its gateway list for a while.
        private IServiceProvider PrimaryServices => ((InProcessSiloHandle)cluster.Primary).SiloHost.Services;

        private IGrainFactory Grains => PrimaryServices.GetRequiredService<IGrainFactory>();

        private IDatastoreGrain Sequencer => Grains.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key);

        private IDatastore Datastore => PrimaryServices.GetRequiredService<IDatastore>();

        public async Task Execute()
        {
            var head = await Call(Sequencer.GetHead());
            Record(head.Head, (await _reference.HeadRevision()).Revision);

            // Every run injects each fault at least once, at seeded positions; the rest of the schedule
            // is drawn step by step.
            var sequencerFaultAt = _rng.Next(options.Steps);
            var shardFaultAt = _rng.Next(options.Steps);
            var deactivateAt = _rng.Next(options.Steps);
            var killAt = options.KillSilos ? _rng.Next(options.Steps) : -1;
            var gcAt = options.GcWindow is null ? -1 : _rng.Next(options.Steps / 2, options.Steps);

            for (var step = 0; step < options.Steps; step++)
            {
                var roll = _rng.NextDouble();
                var extra = _rng.NextDouble();
                // Random kills leave a silo for the seeded kill that takes the sequencer down.
                if (step == killAt && cluster.SecondarySilos.Count > 1)
                    await KillSilo(withSequencer: true);
                else if (options.KillSilos && extra < 0.04 && cluster.SecondarySilos.Count > 2)
                    await KillSilo(withSequencer: false);

                if (step == gcAt || (options.GcWindow is not null && extra > 0.9))
                {
                    await RunGc();
                }

                if (step == sequencerFaultAt || (step != shardFaultAt && roll < 0.06))
                {
                    Trace.Add("fault sequencer-write");
                    StorageFaults.FailSequencerWrites(1);
                    await CommitStep();
                }
                else if (step == shardFaultAt || roll < 0.12)
                {
                    Trace.Add("fault shard-write");
                    StorageFaults.FailShardWrites(1);
                    await Flush();
                }
                else if (step == deactivateAt || roll < 0.18)
                {
                    await DeactivateAll();
                }
                else if (roll < 0.25)
                {
                    await Flush();
                }
                else if (roll < 0.45)
                {
                    await GroupStep();
                }
                else
                {
                    await CommitStep();
                }

                // A fault armed for this step never leaks into a later one.
                StorageFaults.FailSequencerWrites(0);
                StorageFaults.FailShardWrites(0);
                await SpotCheck();
            }

            // A shard-projection failure that certainly fires: one more commit leaves events pending, so
            // the flush has shards to write. Then recovery from storage alone: project everything, drop
            // every activation, and compare every resource at every committed revision and every subject
            // at a sample of them.
            await CommitStep();
            Trace.Add("fault shard-write");
            StorageFaults.FailShardWrites(1);
            await Flush();
            StorageFaults.FailShardWrites(0);
            await Flush();
            Trace.Add("deactivate (final)");
            await DeactivateAll(record: false);
            for (var i = 0; i < _revisions.Count; i++)
            {
                foreach (var doc in Docs)
                    await CheckResource(i, doc);
                if (i % 5 == 0 || i == _revisions.Count - 1)
                {
                    foreach (var user in Users)
                        await CheckSubject(i, user);
                }
            }
        }

        private async Task CommitStep()
        {
            var commit = NextCommit(Docs[_rng.Next(Docs.Length)]);
            var reply = await Call(Sequencer.Commit(commit.Request));
            await Reconcile([(commit, reply)]);
        }

        /// <summary>
        /// Two to four commits on distinct resources, issued together so the sequencer can batch them,
        /// with reads at an already committed revision racing them.
        /// </summary>
        private async Task GroupStep()
        {
            var docs = Docs.OrderBy(_ => _rng.Next()).Take(_rng.Next(2, 5)).ToArray();
            var commits = docs.Select(NextCommit).ToArray();
            Trace.Add($"group of {commits.Length}");

            var readAt = _rng.Next(_revisions.Count);
            var readDoc = Docs[_rng.Next(Docs.Length)];
            var racingRead = CheckResource(readAt, readDoc);
            var replies = await Call(Task.WhenAll(commits.Select(c => Sequencer.Commit(c.Request))));
            await racingRead;
            await Reconcile(commits.Zip(replies).ToList());
        }

        private async Task Flush()
        {
            Trace.Add("flush");
            try
            {
                await Call(Sequencer.FlushProjection());
            }
            catch (Exception ex) when (ex.ToString().Contains("(fault harness)", StringComparison.Ordinal))
            {
                // The injected failure; the events stay pending and a later flush projects them.
            }
        }

        /// <summary>
        /// Hard-kills a secondary silo, whatever activations it held, and waits until the cluster has noticed
        /// and the sequencer answers again. With <paramref name="withSequencer"/>, the sequencer is first
        /// migrated onto the victim, so the kill takes the serialization point down with it.
        /// </summary>
        private async Task KillSilo(bool withSequencer)
        {
            Trace.Add(withSequencer ? "kill silo with the sequencer" : "kill silo");
            var secondaries = cluster.SecondarySilos.ToList();
            var victim = secondaries[_rng.Next(secondaries.Count)];
            var management = Grains.GetGrain<IManagementGrain>(0);
            if (withSequencer)
            {
                // Migration completes once the activation is idle, so wait to see it land.
                await Call(Sequencer.GetHead());
                await Call(cluster.MigrateAsync(Sequencer, victim.SiloAddress));
                var migrateBy = DateTime.UtcNow + CallTimeout;
                while (await Call(management.GetActivationAddress(Sequencer).AsTask()) != victim.SiloAddress)
                {
                    if (DateTime.UtcNow > migrateBy)
                        Diverge("the sequencer did not migrate onto the silo chosen to kill");
                    await Call(Sequencer.GetHead());
                    await Task.Delay(50);
                }
            }

            if (await Call(management.GetActivationAddress(Sequencer).AsTask()) == victim.SiloAddress)
                SequencerKills++;
            await cluster.KillSiloAsync(victim);
            await cluster.WaitForLivenessToStabilizeAsync();
            SilosKilled++;

            // GetHead is a pure read, so retrying it while the directory forgets the dead silo is safe.
            var deadline = DateTime.UtcNow + CallTimeout;
            while (true)
            {
                try
                {
                    await Sequencer.GetHead().WaitAsync(TimeSpan.FromSeconds(5));
                    return;
                }
                catch (Exception) when (DateTime.UtcNow < deadline)
                {
                    await Task.Delay(100);
                }
            }
        }

        private async Task RunGc()
        {
            Trace.Add("gc");
            if (await Call(Sequencer.RunGc()) is { } floor)
            {
                _gcFloor = floor;
                GcRounds++;
            }
        }

        private async Task DeactivateAll(bool record = true)
        {
            if (record)
                Trace.Add("deactivate");
            var management = Grains.GetGrain<IManagementGrain>(0);
            await Call(management.ForceActivationCollection(TimeSpan.Zero));
        }

        /// <summary>
        /// Replays the step's commits against the reference in the order the sequencer committed them
        /// (rejections last: they touch disjoint resources, so their position cannot matter), checking
        /// each reaches the same outcome.
        /// </summary>
        private async Task Reconcile(IReadOnlyList<(HarnessCommit Commit, CommitReply Reply)> results)
        {
            var ordered = results
                .OrderBy(r => r.Reply.Revision ?? long.MaxValue)
                .ToList();
            foreach (var (commit, reply) in ordered)
            {
                var (expected, referenceRevision) = await ReferenceCommit.Apply(_reference, commit.Request);
                if (Outcome(reply) != Outcome(expected))
                {
                    Diverge($"commit {commit.Description}: sequencer replied {Outcome(reply)}, " +
                            $"reference {Outcome(expected)}");
                }

                if (reply.Revision is { } revision)
                    Record(revision, referenceRevision!);
            }

            // Trace in generation order, so concurrent completion order never shows.
            foreach (var (commit, reply) in results)
                Trace.Add($"commit {commit.Description} -> {Outcome(reply)}");
        }

        private async Task SpotCheck()
        {
            var latest = _revisions.Count - 1;
            await CheckResource(latest, Docs[_rng.Next(Docs.Length)]);
            await CheckSubject(latest, Users[_rng.Next(Users.Length)]);
            await CheckResource(_rng.Next(_revisions.Count), Docs[_rng.Next(Docs.Length)]);
        }

        private Task CheckResource(int revisionIndex, string doc) =>
            Compare(
                revisionIndex,
                $"resource doc:{doc}",
                reader => reader.QueryRelationships(new RelationshipsFilter
                {
                    OptionalResourceType = "doc",
                    OptionalResourceIds = [doc],
                }));

        private Task CheckSubject(int revisionIndex, string user) =>
            Compare(
                revisionIndex,
                $"subject user:{user}",
                reader => reader.ReverseQueryRelationships(new SubjectsFilter("user", [user])));

        private async Task Compare(
            int revisionIndex, string what, Func<IDatastoreReader, IAsyncEnumerable<Relationship>> query)
        {
            var revision = _revisions[revisionIndex];
            if (revision < _gcFloor)
                return; // collected: the floor no longer serves this revision
            var actual = await Call(Collect(query(Datastore.SnapshotReader(new TimestampRevision(revision)))));
            var expected = await Collect(query(_reference.SnapshotReader(_referenceAt[revision])));
            if (!actual.SequenceEqual(expected))
            {
                Diverge($"{what} at committed revision #{revisionIndex}: mesh [{string.Join(", ", actual)}], " +
                        $"reference [{string.Join(", ", expected)}]");
            }
        }

        private static async Task<List<string>> Collect(IAsyncEnumerable<Relationship> rows)
        {
            var result = new List<string>();
            await foreach (var row in rows)
                result.Add(row.ToString());
            result.Sort(StringComparer.Ordinal);
            return result;
        }

        private void Record(long revision, IRevision referenceRevision)
        {
            _revisions.Add(revision);
            _referenceAt[revision] = referenceRevision;
        }

        private HarnessCommit NextCommit(string doc)
        {
            var kind = _rng.NextDouble();
            if (kind < 0.2)
            {
                var filter = DocFilter(doc, _rng.Next(2) == 0 ? null : Users[_rng.Next(Users.Length)]);
                ulong? limit = _rng.Next(3) == 0 ? (ulong)_rng.Next(1, 3) : null;
                return new HarnessCommit(
                    $"delete-by-filter {Describe(filter)} limit {limit?.ToString() ?? "none"}",
                    Request([], [], new DeleteByFilterWire(WireConvert.ToFullFilter(filter), limit)));
            }

            var users = Users.OrderBy(_ => _rng.Next()).Take(_rng.Next(1, 4)).ToArray();
            var updates = users.Select(user =>
            {
                var op = _rng.Next(4) switch
                {
                    0 => RelationshipUpdateOpWire.Create,
                    1 => RelationshipUpdateOpWire.Delete,
                    _ => RelationshipUpdateOpWire.Touch,
                };
                return new RelationshipUpdateWire(op, WireConvert.ToWire(Viewer(doc, user)));
            }).ToArray();

            var preconditions = new List<CommitPreconditionWire>();
            if (kind < 0.4)
            {
                var filter = DocFilter(doc, _rng.Next(2) == 0 ? null : Users[_rng.Next(Users.Length)]);
                preconditions.Add(new CommitPreconditionWire(WireConvert.ToFullFilter(filter), _rng.Next(2) == 0));
            }

            var description = string.Join(" ", preconditions.Select(p =>
                    $"{(p.MustMatch ? "must-match" : "must-not-match")} {Describe(WireConvert.ToCoreFilter(p.Filter))}")
                .Concat(updates.Select(u => $"{u.Operation} {doc}#{u.Relationship.SubjectId}")));
            return new HarnessCommit(description, Request(preconditions, updates, null));
        }

        private static RelationshipsFilter DocFilter(string doc, string? user) => new()
        {
            OptionalResourceType = "doc",
            OptionalResourceIds = [doc],
            OptionalResourceRelation = "viewer",
            OptionalSubjectsSelectors = user is null ? null : [new SubjectsSelector("user", [user])],
        };

        private static string Describe(RelationshipsFilter filter) =>
            $"doc:{filter.OptionalResourceIds![0]}" +
            (filter.OptionalSubjectsSelectors is { } s ? $"@user:{s[0].OptionalSubjectIds![0]}" : "");

        private static Relationship Viewer(string doc, string user) =>
            Relationship.Create(
                new ObjectAndRelation("doc", doc, "viewer"),
                new ObjectAndRelation("user", user, CoreConstants.Ellipsis));

        private static CommitRequest Request(
            IReadOnlyList<CommitPreconditionWire> preconditions,
            IReadOnlyList<RelationshipUpdateWire> updates,
            DeleteByFilterWire? deleteByFilter) =>
            new(preconditions, updates, deleteByFilter, SchemaBytes: null, ExpectedSchemaHash: null,
                Array.Empty<CounterDeltaWire>(), ExpectedHead: null);

        private static string Outcome(CommitReply reply) =>
            reply.Failure is { } failure
                ? failure.Kind.ToString()
                : reply.DeletedCount > 0 || reply.ReachedLimit
                    ? $"ok deleted {reply.DeletedCount}{(reply.ReachedLimit ? " limit reached" : "")}"
                    : "ok";

        private async Task<T> Call<T>(Task<T> task)
        {
            try
            {
                return await task.WaitAsync(CallTimeout);
            }
            catch (TimeoutException)
            {
                Diverge("a call did not complete within the timeout");
                throw;
            }
        }

        private async Task Call(Task task)
        {
            try
            {
                await task.WaitAsync(CallTimeout);
            }
            catch (TimeoutException)
            {
                Diverge("a call did not complete within the timeout");
                throw;
            }
        }

        private void Diverge(string what) =>
            throw new FaultHarnessDivergenceException(
                $"seed {options.Seed}: {what}\ntrace:\n  {string.Join("\n  ", Trace)}");
    }

    private sealed record HarnessCommit(string Description, CommitRequest Request);

    private sealed class HarnessSiloConfigurator : ISiloConfigurator
    {
        /// <summary>The run's GC retention window, handed to the in-process silos (runs are serialized).</summary>
        public static TimeSpan? GcWindow;

        public void Configure(ISiloBuilder silo)
        {
            // Production registrations, with the "datastore" provider replaced by the fault injector over
            // storage every silo shares.
            silo.AddStateStorageBasedLogConsistencyProvider("StateStorage");
            silo.AddGraphLocalityPlacement();
            silo.ConfigureServices(services =>
            {
                services.AddKeyedSingleton<IGrainStorage>(
                    "datastore",
                    (sp, _) => new FaultInjectingGrainStorage(new SharedMemoryGrainStorage(sp.GetRequiredService<Serializer>())));
                services.AddSpiceportGrainServices(Schema);
                services.AddSingleton<IDatastore>(sp =>
                    new GrainBackedDatastore(sp.GetRequiredService<IGrainFactory>(), sp.GetRequiredService<LogWatchHub>()));
                if (GcWindow is { } window)
                {
                    // GC runs only when the harness calls RunGc, never on the grain's own reminder.
                    services.AddSingleton<IOptions<DatastoreGcOptions>>(Options.Create(
                        new DatastoreGcOptions { Window = window, ReminderEnabled = false }));
                }
            });
        }
    }
}
