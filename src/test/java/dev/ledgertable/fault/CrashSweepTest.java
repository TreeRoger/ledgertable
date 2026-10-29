package dev.ledgertable.fault;

import dev.ledgertable.TestEvents;
import dev.ledgertable.core.LedgerTable;
import dev.ledgertable.core.Snapshot;
import dev.ledgertable.core.TableOptions;
import dev.ledgertable.core.UsageEvent;
import dev.ledgertable.fault.FaultInjectingObjectStore.Op;
import dev.ledgertable.log.LogKeys;
import dev.ledgertable.store.LocalObjectStore;
import dev.ledgertable.store.ObjectStore;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Kills or fails the writer at every point of the write path, then recovers the way a restarted
 * process would and checks that every batch ends up in the table exactly once and compaction
 * never changes the rows.
 *
 * <p>In each scenario a competing writer steals version 4 just before the victim tries to write
 * it, so conflict detection and rebasing are on the path under test. The victim then commits
 * version 5, and checkpoints every 5 versions put the checkpoint write on the path as well.
 */
class CrashSweepTest {
    private static final TableOptions OPTIONS = TableOptions.DEFAULT.withCheckpointInterval(5);
    private static final String PRODUCER = "victim";

    @TempDir
    Path tmp;

    enum Workload { APPEND, COMPACT }

    @ParameterizedTest(name = "{0} crash at {1}")
    @MethodSource("crashPoints")
    void killAtNamedCrashPoint(Workload workload, CrashPoint point) throws Exception {
        ObjectStore base = new LocalObjectStore(Files.createTempDirectory(tmp, "t"));
        var expected = seed(base);
        var reached = new AtomicBoolean();
        Faults crashOnce = p -> {
            if (p == point && reached.compareAndSet(false, true)) {
                throw new SimulatedCrash(p.name());
            }
        };
        ObjectStore racing = withCompetingCommit(base, expected);
        LedgerTable victim = LedgerTable.open(racing, OPTIONS, crashOnce);
        boolean acknowledged = false;
        try {
            run(workload, victim);
            acknowledged = true;
        } catch (SimulatedCrash expectedCrash) {
            // The process died here; recovery below starts from whatever reached storage.
        }
        assertTrue(reached.get(), point + " was never reached by " + workload);
        recoverAndVerify(base, workload, expected, acknowledged);
    }

    static Stream<Arguments> crashPoints() {
        var args = new ArrayList<Arguments>();
        for (CrashPoint p : CrashPoint.values()) {
            args.add(Arguments.of(Workload.COMPACT, p));
            if (p != CrashPoint.COMPACTION_AFTER_READ) {
                args.add(Arguments.of(Workload.APPEND, p));
            }
        }
        return args.stream();
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Workload.class)
    void injectEveryStorageFaultAtEveryStorageOperation(Workload workload) throws Exception {
        int ops = countStorageOps(workload);
        assertTrue(ops > 5, "suspiciously short write path: " + ops);
        for (int target = 0; target < ops; target++) {
            for (StorageFault fault : StorageFault.values()) {
                if (fault == StorageFault.NONE) {
                    continue;
                }
                ObjectStore base = new LocalObjectStore(Files.createTempDirectory(tmp, "s"));
                var expected = seed(base);
                int victimOp = target;
                var counter = new AtomicInteger();
                ObjectStore faulty = new FaultInjectingObjectStore(withCompetingCommit(base, expected),
                        (op, key) -> counter.getAndIncrement() == victimOp ? fault : StorageFault.NONE);
                boolean acknowledged = false;
                try {
                    run(workload, LedgerTable.open(faulty, OPTIONS, Faults.NONE));
                    acknowledged = true;
                } catch (IOException | SimulatedCrash e) {
                    // Unknown outcome or a dead process: recovery must sort it out either way.
                }
                try {
                    recoverAndVerify(base, workload, expected, acknowledged);
                } catch (AssertionError e) {
                    throw new AssertionError(workload + ": " + fault + " at storage op " + victimOp, e);
                }
            }
        }
    }

    /** Commits three small batches from other producers; returns event id to account, for every expected row. */
    private Map<String, String> seed(ObjectStore store) throws IOException {
        LedgerTable t = LedgerTable.open(store, OPTIONS, Faults.NONE);
        var expected = new HashMap<String, String>();
        for (int i = 1; i <= 3; i++) {
            expected.putAll(record(TestEvents.batch("seed" + i, 1, 3)));
            t.append("seed" + i, 1, TestEvents.batch("seed" + i, 1, 3));
        }
        return expected;
    }

    /**
     * Makes another writer win version 4 just before the victim's first attempt to write it. The
     * competing batch joins the expected rows.
     */
    private ObjectStore withCompetingCommit(ObjectStore base, Map<String, String> expected) {
        var fired = new AtomicBoolean();
        return new FaultInjectingObjectStore(base, (op, key) -> {
            if (op == Op.PUT_IF_ABSENT && key.equals(LogKeys.commit(4)) && fired.compareAndSet(false, true)) {
                try {
                    var rival = TestEvents.batch("rival", 1, 2);
                    expected.putAll(record(rival));
                    LedgerTable.open(base, OPTIONS.withCheckpointInterval(0), Faults.NONE).append("rival", 1, rival);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
            return StorageFault.NONE;
        });
    }

    private static void run(Workload workload, LedgerTable t) throws IOException {
        if (workload == Workload.APPEND) {
            t.append(PRODUCER, 1, TestEvents.batch(PRODUCER, 1, 4));
        } else {
            t.compact(100, 10);
        }
    }

    private int countStorageOps(Workload workload) throws IOException {
        ObjectStore base = new LocalObjectStore(Files.createTempDirectory(tmp, "count"));
        var expected = seed(base);
        var counter = new AtomicInteger();
        ObjectStore counting = new FaultInjectingObjectStore(withCompetingCommit(base, expected), (op, key) -> {
            counter.incrementAndGet();
            return StorageFault.NONE;
        });
        run(workload, LedgerTable.open(counting, OPTIONS, Faults.NONE));
        return counter.get();
    }

    /**
     * Restarts with a fresh, fault-free instance, finishes the interrupted work, and checks the
     * table. An acknowledged append must already be durable before any resubmission.
     */
    private void recoverAndVerify(ObjectStore store, Workload workload, Map<String, String> expected,
                                  boolean acknowledged) throws IOException {
        LedgerTable recovered = LedgerTable.open(store, OPTIONS, Faults.NONE);
        Snapshot afterCrash = recovered.latestSnapshot();
        if (workload == Workload.APPEND) {
            long committed = afterCrash.producer(PRODUCER).map(s -> s.seq()).orElse(0L);
            assertTrue(committed == 1 || !acknowledged, "acknowledged batch is missing");
            // A restarted producer resubmits its in-flight batch whether or not it committed.
            var resubmit = recovered.append(PRODUCER, 1, TestEvents.batch(PRODUCER, 1, 4));
            assertEquals(committed == 1, resubmit.duplicate());
            expected.putAll(record(TestEvents.batch(PRODUCER, 1, 4)));
        } else {
            assertRows(recovered, afterCrash, expected);
            recovered.compact(100, 10);
        }
        Snapshot fin = recovered.latestSnapshot();
        assertRows(recovered, fin, expected);
        assertEquals(fin.toCheckpoint(), LedgerTable.open(new NoCheckpointStore(store)).latestSnapshot().toCheckpoint(),
                "checkpointed state must equal a full replay of the log");
    }

    private static void assertRows(LedgerTable t, Snapshot s, Map<String, String> expected) throws IOException {
        var actual = new HashMap<String, String>();
        List<UsageEvent> rows = t.readAll(s);
        for (UsageEvent e : rows) {
            actual.put(e.eventId(), e.accountId());
        }
        assertEquals(rows.size(), actual.size(), "duplicate rows at version " + s.version());
        assertEquals(expected, actual, "rows at version " + s.version());
    }

    private static Map<String, String> record(List<UsageEvent> events) {
        var m = new HashMap<String, String>();
        events.forEach(e -> m.put(e.eventId(), e.accountId()));
        return m;
    }

    /** A view of the store that hides checkpoints, forcing a full replay. */
    private record NoCheckpointStore(ObjectStore delegate) implements ObjectStore {
        @Override
        public boolean putIfAbsent(String key, byte[] data) {
            throw new UnsupportedOperationException("read-only view");
        }

        @Override
        public void put(String key, byte[] data) {
            throw new UnsupportedOperationException("read-only view");
        }

        @Override
        public Optional<byte[]> get(String key) throws IOException {
            return key.contains("checkpoint") ? Optional.empty() : delegate.get(key);
        }

        @Override
        public List<String> list(String prefix, String startAfter) throws IOException {
            return delegate.list(prefix, startAfter).stream().filter(k -> !k.contains("checkpoint")).toList();
        }

        @Override
        public void delete(String key) {
            throw new UnsupportedOperationException("read-only view");
        }
    }
}
