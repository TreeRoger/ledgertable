package dev.ledgertable.harness;

import dev.ledgertable.billing.Billing;
import dev.ledgertable.billing.LineItem;
import dev.ledgertable.core.CompactionResult;
import dev.ledgertable.core.LedgerTable;
import dev.ledgertable.core.Snapshot;
import dev.ledgertable.core.TableOptions;
import dev.ledgertable.core.UsageEvent;
import dev.ledgertable.fault.Faults;
import dev.ledgertable.fault.SimulatedCrash;
import dev.ledgertable.query.SnapshotQuery;
import dev.ledgertable.store.ObjectStore;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs a workload against one table with concurrent producers, compactors and readers, all
 * subject to random crashes and storage errors, and records everything in a {@link History}
 * for {@code py/ledger/checker.py} to verify.
 */
public final class Harness {

    public record Config(long seed, int compactors, int readers, FaultPlan faults, int checkpointInterval,
                         long smallFileRows, int maxCompactionFiles) {
    }

    private final ObjectStore store;
    private final Config config;
    private final History history;
    private final AtomicBoolean producersDone = new AtomicBoolean();
    private final ConcurrentLinkedQueue<Snapshot> invoiced = new ConcurrentLinkedQueue<>();

    public Harness(ObjectStore store, Config config, History history) {
        this.store = store;
        this.config = config;
        this.history = history;
    }

    /** Runs to completion; returns false if any client hit an error that a correct table can never produce. */
    public boolean run(Map<String, List<Batch>> workload) throws Exception {
        TableOptions options = TableOptions.DEFAULT.withCheckpointInterval(config.checkpointInterval());
        LedgerTable.open(store, options, Faults.NONE);
        history.record("start", "seed", config.seed(), "producers", workload.size(), "compactors", config.compactors(),
                "readers", config.readers(), "crash_rate", config.faults().crashRate(),
                "error_rate", config.faults().errorRate(), "checkpoint_interval", config.checkpointInterval());

        ExecutorService pool = Executors.newCachedThreadPool();
        var producers = new ArrayList<Future<?>>();
        var background = new ArrayList<Future<?>>();
        int index = 0;
        for (var entry : workload.entrySet()) {
            long seed = config.seed() * 1_000_003 + index++;
            var loop = new ProducerLoop(store, options, config.faults(), seed, history, entry.getKey(), entry.getValue(), Faults.NONE);
            producers.add(pool.submit(loop::run));
        }
        for (int c = 0; c < config.compactors(); c++) {
            long seed = config.seed() * 7_000_003 + c;
            String name = "compactor" + c;
            background.add(pool.submit(() -> compactLoop(name, options, seed)));
        }
        for (int r = 0; r < config.readers(); r++) {
            long seed = config.seed() * 9_000_011 + r;
            String name = "reader" + r;
            background.add(pool.submit(() -> readLoop(name, options, seed)));
        }
        boolean healthy = await(producers);
        producersDone.set(true);
        healthy &= await(background);
        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.MINUTES);

        LedgerTable table = LedgerTable.open(store, options, Faults.NONE);
        Snapshot fin = table.latestSnapshot();
        recordRead(table, "final", "final", fin, null);
        var billing = new Billing(table);
        for (Snapshot from : invoiced) {
            history.record("correction", "from", from.version(), "to", fin.version(), "items", items(billing.corrections(from, fin)));
        }
        history.record("final", "version", fin.version());
        return healthy;
    }

    private boolean await(List<Future<?>> futures) throws InterruptedException {
        boolean healthy = true;
        for (var f : futures) {
            try {
                f.get();
            } catch (ExecutionException e) {
                history.record("error", "error", String.valueOf(e.getCause()));
                healthy = false;
            }
        }
        return healthy;
    }

    private Void compactLoop(String name, TableOptions options, long seed) throws InterruptedException {
        var rng = new Random(seed);
        for (int incarnation = 1; !producersDone.get(); incarnation++) {
            String process = name + "#" + incarnation;
            FaultPlan.Process faults = config.faults().incarnation(seed * 31 + incarnation);
            try {
                LedgerTable table = LedgerTable.open(faults.wrap(store), options, faults.crashPoints());
                while (!producersDone.get()) {
                    history.record("invoke", "op", "compact", "process", process);
                    try {
                        CompactionResult r = table.compact(config.smallFileRows(), config.maxCompactionFiles());
                        history.record("ok", "op", "compact", "process", process, "status", r.status().name(),
                                "version", r.version(), "files", r.filesRemoved(), "rows", r.rows());
                    } catch (IOException e) {
                        history.record("info", "op", "compact", "process", process, "error", e.toString());
                    }
                    Thread.sleep(rng.nextInt(5));
                }
            } catch (SimulatedCrash crash) {
                history.record("crash", "process", process, "error", crash.getMessage());
            } catch (IOException e) {
                history.record("crash", "process", process, "error", "failed to start: " + e);
            }
        }
        return null;
    }

    private Void readLoop(String name, TableOptions options, long seed) throws Exception {
        var rng = new Random(seed);
        FaultPlan.Process faults = new FaultPlan(0, config.faults().errorRate()).incarnation(seed);
        ObjectStore flaky = faults.wrap(store);
        LedgerTable table = null;
        long latestSeen = 0;
        long firstCommitTs = LedgerTable.open(store).readCommit(0).orElseThrow().timestampMillis();
        for (int i = 0; !producersDone.get(); i++) {
            long invokedAt = History.now();
            try {
                if (table == null) {
                    table = LedgerTable.open(flaky, options, Faults.NONE);
                }
                switch (rng.nextInt(3)) {
                    case 0 -> {
                        Snapshot s = table.latestSnapshot();
                        latestSeen = s.version();
                        Map<String, Object> extra = Map.of("invoke_t", invokedAt);
                        recordRead(table, name, "latest", s, extra);
                        if (i % 4 == 0) {
                            history.record("invoice", "process", name, "version", s.version(),
                                    "items", items(new Billing(table).statement(s)));
                            invoiced.add(s);
                        }
                    }
                    case 1 -> {
                        long v = latestSeen == 0 ? 0 : rng.nextLong(latestSeen + 1);
                        recordRead(table, name, "version", table.snapshotAt(v), null);
                    }
                    default -> {
                        long ts = firstCommitTs + (long) (rng.nextDouble() * (System.currentTimeMillis() - firstCommitTs));
                        recordRead(table, name, "as_of", table.snapshotAsOf(ts), Map.of("requested_ts", ts));
                    }
                }
            } catch (IOException e) {
                history.record("info", "op", "read", "process", name, "error", e.toString());
            }
            Thread.sleep(rng.nextInt(10));
        }
        return null;
    }

    private void recordRead(LedgerTable table, String process, String mode, Snapshot s, Map<String, Object> extra)
            throws IOException, SQLException {
        List<String> ids = new ArrayList<>();
        for (UsageEvent e : table.readAll(s)) {
            ids.add(e.eventId());
        }
        var totals = new LinkedHashMap<String, Long>();
        try (var q = SnapshotQuery.open(table, s)) {
            for (var row : q.query("SELECT account_id, SUM(compute_millis)::BIGINT AS total FROM usage GROUP BY ALL")) {
                totals.put((String) row.get("account_id"), ((Number) row.get("total")).longValue());
            }
        }
        var fields = new ArrayList<Object>(List.of("process", process, "mode", mode, "version", s.version(),
                "events", ids, "totals", totals));
        if (extra != null) {
            extra.forEach((k, v) -> {
                fields.add(k);
                fields.add(v);
            });
        }
        history.record("read", fields.toArray());
    }

    private static List<List<Object>> items(List<LineItem> items) {
        return items.stream().map(i -> List.<Object>of(i.accountId(), i.day().toEpochDay(), i.computeMillis())).toList();
    }
}
