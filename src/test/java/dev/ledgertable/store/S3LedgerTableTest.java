package dev.ledgertable.store;

import dev.ledgertable.TestEvents;
import dev.ledgertable.billing.Billing;
import dev.ledgertable.billing.LineItem;
import dev.ledgertable.core.LedgerTable;
import dev.ledgertable.core.Snapshot;
import dev.ledgertable.core.TableOptions;
import dev.ledgertable.fault.Faults;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Concurrent writers, compaction, checkpoints and a DuckDB billing query, all against an S3-compatible store. */
class S3LedgerTableTest {
    @Test
    void concurrentWritersAndCompactionOnS3() throws Exception {
        S3Config config = S3Config.fromEnv("table-test/" + UUID.randomUUID());
        Assumptions.assumeTrue(config != null, "LEDGER_S3_ENDPOINT not set");
        var options = TableOptions.DEFAULT.withCheckpointInterval(10);
        int writers = 6;
        int batches = 10;
        ExecutorService pool = Executors.newFixedThreadPool(writers);
        var futures = new ArrayList<Future<?>>();
        for (int w = 0; w < writers; w++) {
            String producer = "w" + w;
            futures.add(pool.submit(() -> {
                try (var store = S3ObjectStore.connect(config)) {
                    var t = LedgerTable.open(store, options, Faults.NONE);
                    for (int seq = 1; seq <= batches; seq++) {
                        t.append(producer, seq, TestEvents.batch(producer, seq, 4));
                        if (seq % 5 == 0) {
                            t.compact(100, 6);
                        }
                    }
                }
                return null;
            }));
        }
        for (var f : futures) {
            f.get();
        }
        pool.shutdown();
        try (var store = S3ObjectStore.connect(config)) {
            var t = LedgerTable.open(store, options, Faults.NONE);
            Snapshot s = t.latestSnapshot();
            var ids = t.readAll(s).stream().map(e -> e.eventId()).toList();
            assertEquals(writers * batches * 4, ids.size());
            assertEquals(ids.size(), new HashSet<>(ids).size());
            long billed = new Billing(t).statement(s).stream().mapToLong(LineItem::computeMillis).sum();
            assertEquals(s.computeMillis(), billed);
            assertEquals(writers * batches * (1000L * 4 + 6), billed);
        }
    }
}
