package dev.ledgertable.core;

import dev.ledgertable.fault.CrashPoint;
import dev.ledgertable.fault.Faults;
import dev.ledgertable.io.ParquetCodec;
import dev.ledgertable.log.AddFile;
import dev.ledgertable.log.Checkpoint;
import dev.ledgertable.log.Commit;
import dev.ledgertable.log.CorruptLogException;
import dev.ledgertable.log.LogCodec;
import dev.ledgertable.log.LogKeys;
import dev.ledgertable.log.Operation;
import dev.ledgertable.log.ProducerMark;
import dev.ledgertable.log.ProducerState;
import dev.ledgertable.store.ObjectStore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * A transactional table of usage events on an {@link ObjectStore}.
 *
 * <p>State is a log of immutable commit objects {@code _log/<v>.json}. A writer reads the latest
 * snapshot at version {@code v}, writes its data files, and then tries to create commit {@code v+1}
 * with put-if-absent. If another writer got there first, it reads the winning commit, checks it
 * for a real conflict, rebases onto it and tries {@code v+2}, and so on.
 *
 * <p>Instances hold no mutable state, so any number of them, in any number of processes, can
 * share one table. A crashed writer is replaced by simply opening a new instance.
 */
public final class LedgerTable {
    private final ObjectStore store;
    private final TableOptions options;
    private final Faults faults;

    private LedgerTable(ObjectStore store, TableOptions options, Faults faults) {
        this.store = store;
        this.options = options;
        this.faults = faults;
    }

    public static LedgerTable open(ObjectStore store) throws IOException {
        return open(store, TableOptions.DEFAULT, Faults.NONE);
    }

    /** Opens the table, creating version 0 if the table does not exist yet. Safe to race. */
    public static LedgerTable open(ObjectStore store, TableOptions options, Faults faults) throws IOException {
        var table = new LedgerTable(store, options, faults);
        if (store.get(LogKeys.commit(0)).isEmpty()) {
            var create = new Commit(0, UUID.randomUUID().toString(), options.clock().getAsLong(), Operation.CREATE, -1,
                    List.of(), List.of(), null);
            store.putIfAbsent(LogKeys.commit(0), LogCodec.encode(create));
        }
        return table;
    }

    public ObjectStore store() {
        return store;
    }

    // ---------------------------------------------------------------- reads

    /** The newest committed state, loaded from the newest checkpoint plus the commits after it. */
    public Snapshot latestSnapshot() throws IOException {
        OptionalLong hint = readCheckpointHint();
        List<String> keys = store.list(LogKeys.VERSIONED_PREFIX,
                hint.isPresent() ? LogKeys.versionPrefix(hint.getAsLong()) : "");
        Optional<Snapshot> snapshot = replay(keys, Long.MAX_VALUE);
        if (snapshot.isEmpty() && hint.isPresent()) {
            snapshot = replay(store.list(LogKeys.VERSIONED_PREFIX), Long.MAX_VALUE);
        }
        return snapshot.orElseThrow(() -> new CorruptLogException("table has no version 0"));
    }

    /** Time travel: the state exactly as of {@code version}. */
    public Snapshot snapshotAt(long version) throws IOException {
        if (version < 0) {
            throw new NoSuchElementException("no version " + version);
        }
        Snapshot s = replay(store.list(LogKeys.VERSIONED_PREFIX), version)
                .orElseThrow(() -> new CorruptLogException("table has no version 0"));
        if (s.version() != version) {
            throw new NoSuchElementException("no version " + version + "; latest is " + s.version());
        }
        return s;
    }

    /** Time travel: the newest state committed at or before {@code timestampMillis}. Commit timestamps strictly increase. */
    public Snapshot snapshotAsOf(long timestampMillis) throws IOException {
        long lo = 0;
        long hi = latestSnapshot().version();
        if (readCommit(0).orElseThrow().timestampMillis() > timestampMillis) {
            throw new NoSuchElementException("table did not exist at " + timestampMillis);
        }
        while (lo < hi) {
            long mid = lo + (hi - lo + 1) / 2;
            if (readCommit(mid).orElseThrow().timestampMillis() <= timestampMillis) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return snapshotAt(lo);
    }

    public Optional<Commit> readCommit(long version) throws IOException {
        return store.get(LogKeys.commit(version)).map(LogCodec::decodeCommit);
    }

    public List<UsageEvent> readFile(AddFile file) throws IOException {
        byte[] bytes = store.get(file.path())
                .orElseThrow(() -> new CorruptLogException("live data file is missing: " + file.path()));
        return ParquetCodec.decode(bytes);
    }

    public List<UsageEvent> readAll(Snapshot snapshot) throws IOException {
        var events = new ArrayList<UsageEvent>();
        for (AddFile f : snapshot.files()) {
            events.addAll(readFile(f));
        }
        return events;
    }

    /**
     * Rebuilds the state at the newest version {@code <= maxVersion} from a sorted listing of log
     * keys: start from the newest usable checkpoint, then apply every later commit in order.
     */
    private Optional<Snapshot> replay(List<String> keys, long maxVersion) throws IOException {
        long checkpointVersion = -1;
        var commitVersions = new ArrayList<Long>();
        for (String key : keys) {
            OptionalLong cp = LogKeys.parseCheckpoint(key);
            if (cp.isPresent() && cp.getAsLong() <= maxVersion) {
                checkpointVersion = cp.getAsLong();
            }
            LogKeys.parseCommit(key).ifPresent(v -> {
                if (v <= maxVersion) {
                    commitVersions.add(v);
                }
            });
        }
        Snapshot base = Snapshot.EMPTY;
        if (checkpointVersion >= 0) {
            Checkpoint cp = LogCodec.decodeCheckpoint(store.get(LogKeys.checkpoint(checkpointVersion))
                    .orElseThrow(() -> new CorruptLogException("listed checkpoint vanished")));
            base = Snapshot.fromCheckpoint(cp);
        }
        var builder = new Snapshot.Builder(base);
        long applied = base.version();
        for (long v : commitVersions) {
            if (v <= applied) {
                continue;
            }
            if (v != applied + 1) {
                throw new CorruptLogException("log gap: version " + (applied + 1) + " missing before " + v);
            }
            Commit c = readCommit(v).orElseThrow(() -> new CorruptLogException("listed commit vanished: " + v));
            builder.apply(c);
            applied = v;
        }
        if (applied < 0) {
            return Optional.empty();
        }
        return Optional.of(builder.build());
    }

    private OptionalLong readCheckpointHint() {
        try {
            return store.get(LogKeys.CHECKPOINT_HINT)
                    .map(b -> OptionalLong.of(Long.parseLong(new String(b, StandardCharsets.UTF_8).trim())))
                    .orElse(OptionalLong.empty());
        } catch (IOException | NumberFormatException e) {
            // The hint only narrows the listing; without it we list the whole log.
            return OptionalLong.empty();
        }
    }

    // ---------------------------------------------------------------- writes

    /**
     * Appends batch {@code seq} of {@code producerId}, exactly once. Producers number their batches
     * 1, 2, 3, ... and submit them in order; resubmitting a batch that already committed (after a
     * timeout, a storage error or a crash) returns a duplicate acknowledgement and changes nothing.
     *
     * @throws OutOfOrderBatchException if {@code seq} skips past the next expected sequence number
     * @throws IOException              if the outcome is unknown; resubmit the same batch to resolve it
     */
    public CommitResult append(String producerId, long seq, List<UsageEvent> events) throws IOException {
        if (events.isEmpty()) {
            throw new IllegalArgumentException("empty batch");
        }
        var ids = new HashSet<String>();
        for (UsageEvent e : events) {
            if (!ids.add(e.eventId())) {
                throw new IllegalArgumentException("duplicate event id within batch: " + e.eventId());
            }
        }
        Snapshot base = latestSnapshot();
        Optional<ProducerState> last = base.producer(producerId);
        long lastSeq = last.map(ProducerState::seq).orElse(0L);
        if (seq <= lastSeq) {
            return new CommitResult(last.get().version(), true, 0);
        }
        if (seq != lastSeq + 1) {
            throw new OutOfOrderBatchException(producerId, lastSeq + 1, seq);
        }

        AddFile file = writeDataFile(events);
        var pending = new PendingCommit(Operation.APPEND, List.of(file), List.of(), new ProducerMark(producerId, seq));
        Outcome outcome = commit(base, pending, winner -> {
            ProducerMark other = winner.producer();
            if (other != null && other.producerId().equals(producerId) && other.seq() >= seq) {
                return Resolution.DUPLICATE;
            }
            return Resolution.REBASE;
        });
        if (outcome.resolution == Resolution.DUPLICATE) {
            deleteQuietly(file.path());
        }
        return new CommitResult(outcome.version, outcome.resolution == Resolution.DUPLICATE, outcome.attempts);
    }

    /**
     * Rewrites up to {@code maxFiles} files smaller than {@code smallFileRows} rows into one file.
     * Appends never conflict with compaction; two compactions conflict only if they remove a common
     * file, and then the later one aborts without changing the table.
     */
    public CompactionResult compact(long smallFileRows, int maxFiles) throws IOException {
        Snapshot base = latestSnapshot();
        List<AddFile> inputs = base.files().stream()
                .filter(f -> f.rows() < smallFileRows)
                .limit(maxFiles)
                .toList();
        if (inputs.size() < 2) {
            return CompactionResult.nothing(base.version());
        }
        var rows = new ArrayList<UsageEvent>();
        for (AddFile f : inputs) {
            rows.addAll(readFile(f));
        }
        faults.at(CrashPoint.COMPACTION_AFTER_READ);
        rows.sort(ROW_ORDER);
        AddFile merged = writeDataFile(rows);
        var removes = inputs.stream().map(AddFile::path).toList();
        var removing = new HashSet<>(removes);
        var pending = new PendingCommit(Operation.COMPACT, List.of(merged), removes, null);
        Outcome outcome = commit(base, pending, winner ->
                winner.removes().stream().anyMatch(removing::contains) ? Resolution.CONFLICT : Resolution.REBASE);
        if (outcome.resolution == Resolution.CONFLICT) {
            deleteQuietly(merged.path());
            return new CompactionResult(CompactionResult.Status.CONFLICT, outcome.version, 0, 0);
        }
        return new CompactionResult(CompactionResult.Status.COMPACTED, outcome.version, inputs.size(), rows.size());
    }

    private static final Comparator<UsageEvent> ROW_ORDER = Comparator
            .comparing(UsageEvent::accountId)
            .thenComparingLong(UsageEvent::eventTimeMillis)
            .thenComparing(UsageEvent::eventId);

    private AddFile writeDataFile(List<UsageEvent> events) throws IOException {
        faults.at(CrashPoint.BEFORE_DATA_WRITE);
        byte[] bytes = ParquetCodec.encode(events);
        String path = LogKeys.DATA_DIR + UUID.randomUUID() + ".parquet";
        if (!store.putIfAbsent(path, bytes)) {
            throw new IllegalStateException("data file name collision: " + path);
        }
        faults.at(CrashPoint.AFTER_DATA_WRITE);
        long compute = 0;
        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;
        for (UsageEvent e : events) {
            compute = Math.addExact(compute, e.computeMillis());
            minTime = Math.min(minTime, e.eventTimeMillis());
            maxTime = Math.max(maxTime, e.eventTimeMillis());
        }
        return new AddFile(path, events.size(), bytes.length, compute, minTime, maxTime);
    }

    // ---------------------------------------------------------------- commit protocol

    private enum Resolution { COMMITTED, REBASE, DUPLICATE, CONFLICT }

    /** Decides what a winning concurrent commit means for the pending one. */
    @FunctionalInterface
    private interface ConflictRule {
        Resolution check(Commit winner);
    }

    private record PendingCommit(Operation operation, List<AddFile> adds, List<String> removes, ProducerMark producer) {
    }

    private record Outcome(Resolution resolution, long version, int attempts) {
    }

    private Outcome commit(Snapshot base, PendingCommit pending, ConflictRule rule) throws IOException {
        String txnId = UUID.randomUUID().toString();
        Snapshot current = base;
        for (int attempt = 1; attempt <= options.maxCommitAttempts(); attempt++) {
            long version = current.version() + 1;
            var commit = new Commit(version, txnId, Math.max(options.clock().getAsLong(), current.timestampMillis() + 1),
                    pending.operation(), base.version(), pending.adds(), pending.removes(), pending.producer());
            faults.at(CrashPoint.BEFORE_COMMIT_WRITE);
            boolean created;
            try {
                created = store.putIfAbsent(LogKeys.commit(version), LogCodec.encode(commit));
            } catch (IOException ambiguous) {
                // The write may or may not have landed; reading the version back settles it.
                created = false;
            }
            Commit winner = commit;
            if (!created) {
                Optional<Commit> existing = readCommit(version);
                if (existing.isEmpty()) {
                    continue;
                }
                winner = existing.get();
            }
            if (winner.txnId().equals(txnId)) {
                faults.at(CrashPoint.AFTER_COMMIT_WRITE);
                maybeCheckpoint(current.apply(winner));
                return new Outcome(Resolution.COMMITTED, version, attempt);
            }
            faults.at(CrashPoint.AFTER_CONFLICT_DETECTED);
            Resolution r = rule.check(winner);
            if (r != Resolution.REBASE) {
                return new Outcome(r, version, attempt);
            }
            current = current.apply(winner);
            backoff(attempt);
        }
        throw new CommitFailedException("lost " + options.maxCommitAttempts() + " commit races in a row");
    }

    private void maybeCheckpoint(Snapshot snapshot) {
        int interval = options.checkpointInterval();
        if (interval <= 0 || snapshot.version() % interval != 0) {
            return;
        }
        try {
            faults.at(CrashPoint.BEFORE_CHECKPOINT_WRITE);
            store.putIfAbsent(LogKeys.checkpoint(snapshot.version()), LogCodec.encode(snapshot.toCheckpoint()));
            faults.at(CrashPoint.AFTER_CHECKPOINT_WRITE);
            store.put(LogKeys.CHECKPOINT_HINT, Long.toString(snapshot.version()).getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            // Checkpoints only speed up reads; the commit itself is already durable.
        }
    }

    private void backoff(int attempt) {
        long cap = Math.min(options.maxBackoffMicros(), 50L << Math.min(attempt, 20));
        if (cap <= 0) {
            return;
        }
        try {
            TimeUnit.MICROSECONDS.sleep(ThreadLocalRandom.current().nextLong(cap + 1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void deleteQuietly(String key) {
        try {
            store.delete(key);
        } catch (IOException e) {
            // An unreferenced data file is invisible to readers; leaving it behind only costs storage.
        }
    }
}
