package dev.ledgertable.bench;

import dev.ledgertable.billing.Billing;
import dev.ledgertable.core.CommitResult;
import dev.ledgertable.core.CompactionResult;
import dev.ledgertable.core.LedgerTable;
import dev.ledgertable.core.TableOptions;
import dev.ledgertable.core.UsageEvent;
import dev.ledgertable.fault.CrashPoint;
import dev.ledgertable.fault.Faults;
import dev.ledgertable.fault.SimulatedCrash;
import dev.ledgertable.log.LogKeys;
import dev.ledgertable.store.ObjectStore;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Micro-benchmarks for the commit path. Each experiment writes raw measurements as CSV, one row
 * per observation, so plots and summaries are computed from the same data that is committed.
 */
public final class Bench {
    private static final int EVENTS_PER_BATCH = 50;
    /** Commits per writer that warm up the JVM and are not measured. */
    private static final int WARMUP_COMMITS = 5;
    private static final long T0 = 1_767_225_600_000L;

    /** Creates an empty store for one experiment; each call must return a fresh, isolated table location. */
    @FunctionalInterface
    public interface StoreFactory {
        ObjectStore create(String name) throws IOException;
    }

    private final StoreFactory stores;
    private final String storeName;
    private final Path out;
    private final boolean quick;

    public Bench(StoreFactory stores, String storeName, Path out, boolean quick) {
        this.stores = stores;
        this.storeName = storeName;
        this.out = out;
        this.quick = quick;
    }

    public void runAll() throws Exception {
        Files.createDirectories(out);
        commitScaling();
        compactionImpact();
        checkpointImpact();
        recoveryCost();
    }

    /** Commit latency, attempts and throughput as the number of concurrent writers grows. */
    void commitScaling() throws Exception {
        int[] writerCounts = quick ? new int[] {1, 4} : new int[] {1, 2, 4, 8, 16};
        int commitsPerWriter = quick ? 20 : 60;
        try (var csv = csv("commit_scaling", "store,writers,writer,start_us,latency_us,attempts")) {
            for (int writers : writerCounts) {
                try (ObjectStore store = stores.create("scaling-" + writers)) {
                    LedgerTable.open(store);
                    var rows = runWriters(store, TableOptions.DEFAULT, writers, commitsPerWriter, null);
                    for (long[] r : rows) {
                        csv.printf("%s,%d,%d,%d,%d,%d%n", storeName, writers, r[0], r[1], r[2], r[3]);
                    }
                }
                System.out.printf("commit_scaling writers=%d done%n", writers);
            }
        }
    }

    /** Append latency with and without concurrent compactors, and billing query latency before and after compaction. */
    void compactionImpact() throws Exception {
        int writers = 4;
        int commitsPerWriter = quick ? 20 : 60;
        try (var csv = csv("compaction_writers", "store,compactors,latency_us,attempts")) {
            for (int compactors : new int[] {0, 2}) {
                try (ObjectStore store = stores.create("compaction-" + compactors)) {
                    LedgerTable.open(store);
                    var rows = runWriters(store, TableOptions.DEFAULT, writers, commitsPerWriter, compactors == 0 ? null : compactors);
                    for (long[] r : rows) {
                        csv.printf("%s,%d,%d,%d%n", storeName, compactors, r[2], r[3]);
                    }
                }
            }
        }
        int files = quick ? 100 : 400;
        int repeats = quick ? 3 : 10;
        try (var csv = csv("compaction_query", "store,phase,files,query_ms"); ObjectStore store = stores.create("query")) {
            LedgerTable table = LedgerTable.open(store, TableOptions.DEFAULT, Faults.NONE);
            for (int seq = 1; seq <= files; seq++) {
                table.append("p", seq, batch("p", seq, 25));
            }
            var billing = new Billing(table);
            measureQuery(csv, billing, table, "before", repeats);
            while (table.compact(10_000, 50).status() == CompactionResult.Status.COMPACTED) {
                // Each round merges up to 50 files; stop once no two small files remain.
            }
            measureQuery(csv, billing, table, "after", repeats);
        }
        System.out.println("compaction done");
    }

    private void measureQuery(PrintWriter csv, Billing billing, LedgerTable table, String phase, int repeats) throws Exception {
        var snapshot = table.latestSnapshot();
        billing.statement(snapshot);
        for (int i = 0; i < repeats; i++) {
            long t = System.nanoTime();
            billing.statement(snapshot);
            csv.printf("%s,%s,%d,%.3f%n", storeName, phase, snapshot.files().size(), (System.nanoTime() - t) / 1e6);
        }
    }

    /** Snapshot load time as the log grows, with checkpoints off and at two intervals. */
    void checkpointImpact() throws Exception {
        int[] lengths = quick ? new int[] {50, 200} : new int[] {100, 500, 1000, 2000};
        int[] intervals = {0, 25, 100};
        int repeats = quick ? 3 : 10;
        try (var csv = csv("checkpoint_load", "store,interval,log_length,load_ms")) {
            for (int interval : intervals) {
                try (ObjectStore store = stores.create("checkpoint-" + interval)) {
                    var options = TableOptions.DEFAULT.withCheckpointInterval(interval);
                    LedgerTable table = LedgerTable.open(store, options, Faults.NONE);
                    int written = 0;
                    for (int length : lengths) {
                        for (; written < length; written++) {
                            table.append("p", written + 1, batch("p", written + 1, 1));
                        }
                        LedgerTable reader = LedgerTable.open(store, options, Faults.NONE);
                        reader.latestSnapshot();
                        for (int i = 0; i < repeats; i++) {
                            long t = System.nanoTime();
                            reader.latestSnapshot();
                            csv.printf("%s,%d,%d,%.3f%n", storeName, interval, length, (System.nanoTime() - t) / 1e6);
                        }
                    }
                }
                System.out.printf("checkpoint interval=%d done%n", interval);
            }
        }
    }

    /**
     * Time for a restarted writer to get its interrupted batch acknowledged: open the table, find
     * its last committed batch, and resubmit. Measured for a crash before the commit (the batch
     * must be rewritten) and after it (the resubmission is a duplicate).
     */
    void recoveryCost() throws Exception {
        int[] lengths = quick ? new int[] {50} : new int[] {100, 1000};
        int repeats = quick ? 3 : 10;
        try (var csv = csv("recovery", "store,interval,log_length,crash_point,recovery_ms,duplicate,orphan_files")) {
            for (int interval : new int[] {0, 25}) {
                for (int length : lengths) {
                    try (ObjectStore store = stores.create("recovery-" + interval + "-" + length)) {
                        var options = TableOptions.DEFAULT.withCheckpointInterval(interval);
                        LedgerTable table = LedgerTable.open(store, options, Faults.NONE);
                        for (int seq = 1; seq <= length; seq++) {
                            table.append("bulk", seq, batch("bulk", seq, 1));
                        }
                        long seq = 0;
                        for (CrashPoint point : List.of(CrashPoint.AFTER_DATA_WRITE, CrashPoint.AFTER_COMMIT_WRITE)) {
                            for (int i = 0; i < repeats; i++) {
                                seq++;
                                var events = batch("victim", seq, EVENTS_PER_BATCH);
                                int orphansBefore = orphans(store, table);
                                try {
                                    LedgerTable.open(store, options, crashAt(point)).append("victim", seq, events);
                                    throw new IllegalStateException("crash point not reached: " + point);
                                } catch (SimulatedCrash expected) {
                                    // The writer is gone; a new process takes over below.
                                }
                                long t = System.nanoTime();
                                LedgerTable restarted = LedgerTable.open(store, options, Faults.NONE);
                                restarted.latestSnapshot().producer("victim");
                                CommitResult r = restarted.append("victim", seq, events);
                                double ms = (System.nanoTime() - t) / 1e6;
                                csv.printf("%s,%d,%d,%s,%.3f,%b,%d%n", storeName, interval, length, point, ms,
                                        r.duplicate(), orphans(store, table) - orphansBefore);
                            }
                        }
                    }
                }
            }
        }
        System.out.println("recovery done");
    }

    /** Data files no snapshot references: what crashed writers leave behind. */
    private static int orphans(ObjectStore store, LedgerTable table) throws IOException {
        return store.list(LogKeys.DATA_DIR).size() - table.latestSnapshot().files().size();
    }

    private static Faults crashAt(CrashPoint point) {
        return p -> {
            if (p == point) {
                throw new SimulatedCrash(p.name());
            }
        };
    }

    /**
     * Returns rows of {writer, start_us, latency_us, attempts} for measured commits, with start
     * relative to the start of the run. With compactors set, that many compactors run alongside.
     */
    private List<long[]> runWriters(ObjectStore store, TableOptions options, int writers, int commitsPerWriter,
                                    Integer compactors) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(writers + (compactors == null ? 0 : compactors));
        var stop = new AtomicBoolean();
        var background = new ArrayList<Future<?>>();
        if (compactors != null) {
            for (int c = 0; c < compactors; c++) {
                background.add(pool.submit(() -> {
                    LedgerTable t = LedgerTable.open(store, options, Faults.NONE);
                    while (!stop.get()) {
                        t.compact(10_000, 8);
                    }
                    return null;
                }));
            }
        }
        var futures = new ArrayList<Future<List<long[]>>>();
        long origin = System.nanoTime();
        for (int w = 0; w < writers; w++) {
            int writer = w;
            futures.add(pool.submit(() -> {
                LedgerTable t = LedgerTable.open(store, options, Faults.NONE);
                var rows = new ArrayList<long[]>();
                for (int seq = 1; seq <= WARMUP_COMMITS + commitsPerWriter; seq++) {
                    var events = batch("w" + writer, seq, EVENTS_PER_BATCH);
                    long start = System.nanoTime();
                    CommitResult r = t.append("w" + writer, seq, events);
                    if (seq > WARMUP_COMMITS) {
                        rows.add(new long[] {writer, (start - origin) / 1_000, (System.nanoTime() - start) / 1_000, r.attempts()});
                    }
                }
                return rows;
            }));
        }
        var all = new ArrayList<long[]>();
        for (var f : futures) {
            all.addAll(f.get());
        }
        stop.set(true);
        for (var f : background) {
            f.get();
        }
        pool.shutdown();
        return all;
    }

    private static List<UsageEvent> batch(String producer, long seq, int n) {
        var rng = new Random(producer.hashCode() * 31L + seq);
        var events = new ArrayList<UsageEvent>(n);
        for (int i = 0; i < n; i++) {
            events.add(new UsageEvent(producer + "-" + seq + "-" + i, "acct-" + rng.nextInt(50), "res-" + rng.nextInt(8),
                    rng.nextInt(3_600_000), T0 + seq * 1_000 + i));
        }
        return events;
    }

    private PrintWriter csv(String name, String header) throws IOException {
        var w = new PrintWriter(Files.newBufferedWriter(out.resolve(name + "_" + storeName + ".csv")));
        w.println(header);
        return w;
    }
}
