package dev.ledgertable.core;

import dev.ledgertable.TestEvents;
import dev.ledgertable.fault.FaultInjectingObjectStore;
import dev.ledgertable.fault.FaultInjectingObjectStore.Op;
import dev.ledgertable.fault.Faults;
import dev.ledgertable.fault.StorageFault;
import dev.ledgertable.log.AddFile;
import dev.ledgertable.log.Commit;
import dev.ledgertable.log.CorruptLogException;
import dev.ledgertable.log.LogCodec;
import dev.ledgertable.log.LogKeys;
import dev.ledgertable.log.Operation;
import dev.ledgertable.store.LocalObjectStore;
import dev.ledgertable.store.ObjectStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LedgerTableTest {
    @TempDir
    Path dir;
    ObjectStore store;
    LedgerTable table;

    @BeforeEach
    void setUp() throws Exception {
        store = new LocalObjectStore(dir);
        table = LedgerTable.open(store);
    }

    @Test
    void newTableIsEmptyAtVersionZero() throws Exception {
        Snapshot s = table.latestSnapshot();
        assertEquals(0, s.version());
        assertEquals(0, s.rowCount());
    }

    @Test
    void openingTwiceDoesNotRecreate() throws Exception {
        long created = table.readCommit(0).orElseThrow().timestampMillis();
        LedgerTable.open(store);
        assertEquals(created, table.readCommit(0).orElseThrow().timestampMillis());
        assertEquals(0, table.latestSnapshot().version());
    }

    @Test
    void appendsAreVisibleInOrder() throws Exception {
        var r1 = table.append("p", 1, TestEvents.batch("p", 1, 10));
        var r2 = table.append("p", 2, TestEvents.batch("p", 2, 5));
        assertEquals(new CommitResult(1, false, 1), r1);
        assertEquals(new CommitResult(2, false, 1), r2);
        Snapshot s = table.latestSnapshot();
        assertEquals(15, s.rowCount());
        assertEquals(new HashSet<>(ids(TestEvents.batch("p", 1, 10), TestEvents.batch("p", 2, 5))),
                new HashSet<>(ids(table.readAll(s))));
    }

    @Test
    void resubmittedBatchIsAcknowledgedButNotRecounted() throws Exception {
        table.append("p", 1, TestEvents.batch("p", 1, 10));
        var retry = table.append("p", 1, TestEvents.batch("p", 1, 10));
        assertTrue(retry.duplicate());
        assertEquals(1, retry.version());
        assertEquals(10, table.latestSnapshot().rowCount());
        assertEquals(1, table.latestSnapshot().version());
    }

    @Test
    void batchesMustNotSkipSequenceNumbers() throws Exception {
        table.append("p", 1, TestEvents.batch("p", 1, 1));
        assertThrows(OutOfOrderBatchException.class, () -> table.append("p", 3, TestEvents.batch("p", 3, 1)));
    }

    @Test
    void rejectsDuplicateIdsWithinABatch() {
        var e = TestEvents.batch("p", 1, 1).getFirst();
        assertThrows(IllegalArgumentException.class, () -> table.append("p", 1, List.of(e, e)));
    }

    @Test
    void timeTravelByVersionAndTimestamp() throws Exception {
        var clock = new AtomicLong(1_000);
        var t = LedgerTable.open(new LocalObjectStore(dir.resolve("tt")), TableOptions.DEFAULT.withClock(clock::get), Faults.NONE);
        for (int seq = 1; seq <= 5; seq++) {
            clock.set(1_000 + seq * 100L);
            t.append("p", seq, TestEvents.batch("p", seq, seq));
        }
        assertEquals(1 + 2 + 3, t.snapshotAt(3).rowCount());
        assertEquals(3, t.snapshotAsOf(1_350).version());
        assertEquals(5, t.snapshotAsOf(Long.MAX_VALUE).version());
        assertEquals(0, t.snapshotAsOf(1_000).version());
        assertThrows(NoSuchElementException.class, () -> t.snapshotAsOf(999));
        assertThrows(NoSuchElementException.class, () -> t.snapshotAt(6));
    }

    @Test
    void commitTimestampsStrictlyIncreaseEvenIfTheClockDoesNot() throws Exception {
        var t = LedgerTable.open(new LocalObjectStore(dir.resolve("clock")), TableOptions.DEFAULT.withClock(() -> 42), Faults.NONE);
        t.append("p", 1, TestEvents.batch("p", 1, 1));
        t.append("p", 2, TestEvents.batch("p", 2, 1));
        assertTrue(t.readCommit(2).orElseThrow().timestampMillis() > t.readCommit(1).orElseThrow().timestampMillis());
    }

    @Test
    void checkpointedLoadMatchesFullReplay() throws Exception {
        var t = LedgerTable.open(store, TableOptions.DEFAULT.withCheckpointInterval(4), Faults.NONE);
        for (int seq = 1; seq <= 11; seq++) {
            t.append("p", seq, TestEvents.batch("p", seq, 3));
        }
        t.compact(100, 5);
        assertTrue(store.get(LogKeys.checkpoint(8)).isPresent());
        Snapshot fromCheckpoint = t.latestSnapshot();
        for (String key : store.list(LogKeys.VERSIONED_PREFIX)) {
            if (LogKeys.parseCheckpoint(key).isPresent()) {
                store.delete(key);
            }
        }
        store.delete(LogKeys.CHECKPOINT_HINT);
        Snapshot replayed = t.latestSnapshot();
        assertEquals(replayed.toCheckpoint(), fromCheckpoint.toCheckpoint());
    }

    @Test
    void timeTravelBeforeACheckpointStillWorks() throws Exception {
        var t = LedgerTable.open(store, TableOptions.DEFAULT.withCheckpointInterval(2), Faults.NONE);
        for (int seq = 1; seq <= 6; seq++) {
            t.append("p", seq, TestEvents.batch("p", seq, 1));
        }
        assertEquals(3, t.snapshotAt(3).rowCount());
        assertEquals(1, t.snapshotAt(1).rowCount());
    }

    @Test
    void compactionPreservesRowsAndRemovesInputs() throws Exception {
        for (int seq = 1; seq <= 6; seq++) {
            table.append("p", seq, TestEvents.batch("p", seq, 4));
        }
        Snapshot before = table.latestSnapshot();
        var result = table.compact(100, 10);
        assertEquals(CompactionResult.Status.COMPACTED, result.status());
        assertEquals(6, result.filesRemoved());
        Snapshot after = table.latestSnapshot();
        assertEquals(1, after.files().size());
        assertEquals(sortedIds(table.readAll(before)), sortedIds(table.readAll(after)));
        assertEquals(before.computeMillis(), after.computeMillis());
        assertEquals(Operation.COMPACT, table.readCommit(after.version()).orElseThrow().operation());
    }

    @Test
    void compactionNeedsAtLeastTwoSmallFiles() throws Exception {
        table.append("p", 1, TestEvents.batch("p", 1, 4));
        assertEquals(CompactionResult.Status.NOTHING_TO_COMPACT, table.compact(100, 10).status());
    }

    @Test
    void concurrentWritersAllCommitExactlyOnce() throws Exception {
        int writers = 8;
        int batches = 25;
        ExecutorService pool = Executors.newFixedThreadPool(writers);
        var futures = new ArrayList<Future<?>>();
        for (int w = 0; w < writers; w++) {
            String producer = "w" + w;
            futures.add(pool.submit(() -> {
                var t = LedgerTable.open(store);
                for (int seq = 1; seq <= batches; seq++) {
                    assertFalse(t.append(producer, seq, TestEvents.batch(producer, seq, 3)).duplicate());
                }
                return null;
            }));
        }
        for (var f : futures) {
            f.get();
        }
        pool.shutdown();
        Snapshot s = table.latestSnapshot();
        assertEquals(writers * batches, s.version());
        List<String> all = ids(table.readAll(s));
        assertEquals(writers * batches * 3, all.size());
        assertEquals(all.size(), new HashSet<>(all).size());
        for (int w = 0; w < writers; w++) {
            assertEquals(batches, s.producer("w" + w).orElseThrow().seq());
        }
    }

    @Test
    void compactionConcurrentWithWritersNeverLosesOrDuplicatesRows() throws Exception {
        int writers = 4;
        int batches = 30;
        ExecutorService pool = Executors.newFixedThreadPool(writers + 2);
        var done = new AtomicBoolean();
        var compactions = new AtomicLong();
        var futures = new ArrayList<Future<?>>();
        for (int c = 0; c < 2; c++) {
            futures.add(pool.submit(() -> {
                var t = LedgerTable.open(store);
                while (!done.get()) {
                    if (t.compact(50, 8).status() == CompactionResult.Status.COMPACTED) {
                        compactions.incrementAndGet();
                    }
                }
                return null;
            }));
        }
        var writerFutures = new ArrayList<Future<?>>();
        for (int w = 0; w < writers; w++) {
            String producer = "w" + w;
            writerFutures.add(pool.submit(() -> {
                var t = LedgerTable.open(store);
                for (int seq = 1; seq <= batches; seq++) {
                    t.append(producer, seq, TestEvents.batch(producer, seq, 2));
                }
                return null;
            }));
        }
        for (var f : writerFutures) {
            f.get();
        }
        done.set(true);
        for (var f : futures) {
            f.get();
        }
        pool.shutdown();
        assertTrue(compactions.get() > 0, "expected at least one compaction to commit");
        List<String> all = ids(table.readAll(table.latestSnapshot()));
        assertEquals(writers * batches * 2, all.size());
        assertEquals(all.size(), new HashSet<>(all).size());
    }

    @Test
    void ambiguousCommitWriteIsResolvedByReadingItBack() throws Exception {
        var flaky = new FaultInjectingObjectStore(store, (op, key) ->
                op == Op.PUT_IF_ABSENT && key.equals(LogKeys.commit(1)) ? StorageFault.ERROR_AFTER : StorageFault.NONE);
        var t = LedgerTable.open(flaky);
        var r = t.append("p", 1, TestEvents.batch("p", 1, 2));
        assertEquals(new CommitResult(1, false, 1), r);
        assertEquals(1, t.latestSnapshot().version());
    }

    @Test
    void writerAbandonedAfterAmbiguousCommitIsDeduplicatedOnResubmit() throws Exception {
        var flaky = new FaultInjectingObjectStore(store, (op, key) ->
                op == Op.PUT_IF_ABSENT && key.startsWith(LogKeys.LOG_DIR) ? StorageFault.ERROR_AFTER : StorageFault.NONE);
        var lost = LedgerTable.open(flaky);
        // The commit lands, but the read-back that would confirm it also fails: the caller sees an error.
        var failingReads = new FaultInjectingObjectStore(flaky, (op, key) ->
                op == Op.GET && key.equals(LogKeys.commit(1)) ? StorageFault.ERROR_BEFORE : StorageFault.NONE);
        assertThrows(IOException.class,
                () -> LedgerTable.open(failingReads).append("p", 1, TestEvents.batch("p", 1, 2)));
        var retry = lost.append("p", 1, TestEvents.batch("p", 1, 2));
        assertTrue(retry.duplicate());
        assertEquals(2, table.latestSnapshot().rowCount());
    }

    @Test
    void listingThatSkipsAConcurrentlyCreatedCommitIsTolerated() throws Exception {
        for (int seq = 1; seq <= 4; seq++) {
            table.append("p", seq, TestEvents.batch("p", seq, 1));
        }
        // readdir and paginated S3 listings may omit a key created while the listing ran.
        ObjectStore skipping = new ListingSkipsStore(store, LogKeys.commit(2));
        var t = LedgerTable.open(skipping);
        assertEquals(4, t.latestSnapshot().rowCount());
        assertEquals(2, t.snapshotAt(2).rowCount());
        assertEquals(5, t.append("p", 5, TestEvents.batch("p", 5, 1)).version());
    }

    @Test
    void timeTravelFindsAVersionTheListingMissed() throws Exception {
        table.append("p", 1, TestEvents.batch("p", 1, 1));
        var t = LedgerTable.open(new ListingSkipsStore(store, LogKeys.commit(1)));
        assertEquals(1, t.snapshotAt(1).rowCount());
    }

    @Test
    void missingCommitBelowAListedOneIsCorruption() throws Exception {
        table.append("p", 1, TestEvents.batch("p", 1, 1));
        table.append("p", 2, TestEvents.batch("p", 2, 1));
        store.delete(LogKeys.commit(1));
        assertThrows(CorruptLogException.class, table::latestSnapshot);
    }

    /** Hides one key from listings while still serving it by key. */
    private record ListingSkipsStore(ObjectStore delegate, String hidden) implements ObjectStore {
        @Override
        public boolean putIfAbsent(String key, byte[] data) throws IOException {
            return delegate.putIfAbsent(key, data);
        }

        @Override
        public void put(String key, byte[] data) throws IOException {
            delegate.put(key, data);
        }

        @Override
        public Optional<byte[]> get(String key) throws IOException {
            return delegate.get(key);
        }

        @Override
        public List<String> list(String prefix, String startAfter) throws IOException {
            return delegate.list(prefix, startAfter).stream().filter(k -> !k.equals(hidden)).toList();
        }

        @Override
        public void delete(String key) throws IOException {
            delegate.delete(key);
        }
    }

    @Test
    void ambiguousWriteToAVersionAnotherWriterTookIsNotClaimed() throws Exception {
        table.append("p", 1, TestEvents.batch("p", 1, 2));
        // Our put of version 2 fails without landing, and meanwhile a rival commits version 2.
        var fired = new AtomicBoolean();
        var flaky = new FaultInjectingObjectStore(store, (op, key) -> {
            if (op == Op.PUT_IF_ABSENT && key.equals(LogKeys.commit(2)) && fired.compareAndSet(false, true)) {
                try {
                    LedgerTable.open(store).append("rival", 1, TestEvents.batch("rival", 1, 3));
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
                return StorageFault.ERROR_BEFORE;
            }
            return StorageFault.NONE;
        });
        var r = LedgerTable.open(flaky).append("p", 2, TestEvents.batch("p", 2, 2));
        assertEquals(new CommitResult(3, false, 2), r, "version 2 exists but holds the rival's txn, so we rebase to 3");
        assertEquals("rival", table.readCommit(2).orElseThrow().producer().producerId());
        assertEquals("p", table.readCommit(3).orElseThrow().producer().producerId());
        assertEquals(7, table.latestSnapshot().rowCount());
    }

    @Test
    void producerDedupStateSurvivesCompactionAndCheckpoints() throws Exception {
        var t = LedgerTable.open(store, TableOptions.DEFAULT.withCheckpointInterval(5), Faults.NONE);
        for (int seq = 1; seq <= 3; seq++) {
            t.append("p", seq, TestEvents.batch("p", seq, 4));                    // v1-v3
        }
        assertEquals(CompactionResult.Status.COMPACTED, t.compact(100, 10).status()); // v4 rewrites p's files
        t.append("q", 1, TestEvents.batch("q", 1, 4));                             // v5, checkpointed
        assertTrue(store.get(LogKeys.checkpoint(5)).isPresent());

        // Serve only commit 0, the checkpoint and later commits, so the producer state can only come from the checkpoint.
        var fromCheckpoint = LedgerTable.open(new HidingStore(store, k -> {
            var v = LogKeys.parseCommit(k);
            return v.isPresent() && v.getAsLong() >= 1 && v.getAsLong() <= 4;
        }));
        var retry = fromCheckpoint.append("p", 3, TestEvents.batch("p", 3, 4));
        assertEquals(new CommitResult(3, true, 0), retry, "the retried batch is recognised, with its original version");
        assertTrue(fromCheckpoint.append("p", 2, TestEvents.batch("p", 2, 4)).duplicate());
        assertThrows(OutOfOrderBatchException.class, () -> fromCheckpoint.append("p", 5, TestEvents.batch("p", 5, 1)));
        assertEquals(new CommitResult(6, false, 1), fromCheckpoint.append("p", 4, TestEvents.batch("p", 4, 4)));
        assertEquals(20, t.latestSnapshot().rowCount());
    }

    @Test
    void retriedBatchRacingPastACheckpointIsDeduplicatedDuringRebase() throws Exception {
        var options = TableOptions.DEFAULT.withCheckpointInterval(2);
        var t = LedgerTable.open(store, options, Faults.NONE);
        t.append("p", 1, TestEvents.batch("p", 1, 2));
        // A stalled process for p reads v1, then p's restarted process commits batch 2 at v2 (a checkpoint)
        // and other writers move on; when the stalled one finally writes, it must rebase into a duplicate.
        var fired = new AtomicBoolean();
        var stalled = new FaultInjectingObjectStore(store, (op, key) -> {
            if (op == Op.PUT_IF_ABSENT && key.equals(LogKeys.commit(2)) && fired.compareAndSet(false, true)) {
                try {
                    var restarted = LedgerTable.open(store, options, Faults.NONE);
                    restarted.append("p", 2, TestEvents.batch("p", 2, 2));
                    restarted.append("q", 1, TestEvents.batch("q", 1, 2));
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
            return StorageFault.NONE;
        });
        var r = LedgerTable.open(stalled, options, Faults.NONE).append("p", 2, TestEvents.batch("p", 2, 2));
        assertTrue(store.get(LogKeys.checkpoint(2)).isPresent());
        assertEquals(new CommitResult(2, true, 1), r);
        assertEquals(6, t.latestSnapshot().rowCount());
        assertEquals(3, t.latestSnapshot().version());
    }

    /** Hides matching keys from both reads and listings. */
    private record HidingStore(ObjectStore delegate, Predicate<String> hidden) implements ObjectStore {
        @Override
        public boolean putIfAbsent(String key, byte[] data) throws IOException {
            return delegate.putIfAbsent(key, data);
        }

        @Override
        public void put(String key, byte[] data) throws IOException {
            delegate.put(key, data);
        }

        @Override
        public Optional<byte[]> get(String key) throws IOException {
            return hidden.test(key) ? Optional.empty() : delegate.get(key);
        }

        @Override
        public List<String> list(String prefix, String startAfter) throws IOException {
            return delegate.list(prefix, startAfter).stream().filter(k -> !hidden.test(k)).toList();
        }

        @Override
        public void delete(String key) throws IOException {
            delegate.delete(key);
        }
    }

    @Test
    void corruptLogIsRejected() throws Exception {
        table.append("p", 1, TestEvents.batch("p", 1, 1));
        AddFile ghost = new AddFile("data/ghost.parquet", 1, 1, 1, 0, 0);
        var bad = new Commit(2, "x", Long.MAX_VALUE, Operation.COMPACT, 1, List.of(ghost), List.of("data/never-added"), null);
        store.putIfAbsent(LogKeys.commit(2), LogCodec.encode(bad));
        assertThrows(CorruptLogException.class, table::latestSnapshot);
    }

    static List<String> ids(List<UsageEvent> events) {
        return events.stream().map(UsageEvent::eventId).toList();
    }

    @SafeVarargs
    static List<String> ids(List<UsageEvent>... batches) {
        var all = new ArrayList<String>();
        for (var b : batches) {
            all.addAll(ids(b));
        }
        return all;
    }

    static List<String> sortedIds(List<UsageEvent> events) {
        return ids(events).stream().sorted().toList();
    }
}
