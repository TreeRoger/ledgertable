package dev.ledgertable.billing;

import dev.ledgertable.TestEvents;
import dev.ledgertable.core.LedgerTable;
import dev.ledgertable.core.Snapshot;
import dev.ledgertable.core.UsageEvent;
import dev.ledgertable.query.SnapshotQuery;
import dev.ledgertable.store.LocalObjectStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BillingTest {
    private static final LocalDate JAN1 = LocalDate.ofEpochDay(TestEvents.T0 / TestEvents.DAY);

    @TempDir
    Path dir;
    LedgerTable table;
    Billing billing;

    @BeforeEach
    void setUp() throws Exception {
        table = LedgerTable.open(new LocalObjectStore(dir));
        billing = new Billing(table);
    }

    @Test
    void emptyTableHasEmptyStatement() throws Exception {
        assertEquals(List.of(), billing.statement(table.latestSnapshot()));
    }

    @Test
    void statementSumsPerAccountAndDay() throws Exception {
        table.append("p", 1, List.of(
                event("a", "acme", 100, 0), event("b", "acme", 250, 0),
                event("c", "acme", 7, 1), event("d", "zeta", 1, 0)));
        assertEquals(List.of(
                new LineItem("acme", JAN1, 350), new LineItem("acme", JAN1.plusDays(1), 7),
                new LineItem("zeta", JAN1, 1)), billing.statement(table.latestSnapshot()));
    }

    @Test
    void lateEventsProduceCorrectionsForAlreadyInvoicedDays() throws Exception {
        table.append("p", 1, List.of(event("a", "acme", 100, 0), event("b", "zeta", 40, 0)));
        Snapshot invoiced = table.latestSnapshot();
        table.append("p", 2, List.of(event("c", "acme", 900, 3)));
        table.append("late", 1, List.of(event("d", "acme", 25, 0), event("e", "zeta", 2, 0)));
        Snapshot now = table.latestSnapshot();
        assertEquals(List.of(
                new LineItem("acme", JAN1, 25), new LineItem("acme", JAN1.plusDays(3), 900),
                new LineItem("zeta", JAN1, 2)), billing.corrections(invoiced, now));
    }

    @Test
    void compactionProducesNoCorrections() throws Exception {
        for (int seq = 1; seq <= 4; seq++) {
            table.append("p", seq, TestEvents.batch("p", seq, 5));
        }
        Snapshot before = table.latestSnapshot();
        table.compact(100, 10);
        assertEquals(List.of(), billing.corrections(before, table.latestSnapshot()));
    }

    @Test
    void invoicePlusCorrectionsEqualsLaterStatementAndGroundTruth() throws Exception {
        var rng = new Random(7);
        var truth = new HashMap<LineKey, Long>();
        var snapshots = new ArrayList<Snapshot>();
        snapshots.add(table.latestSnapshot());
        int id = 0;
        for (int seq = 1; seq <= 12; seq++) {
            var batch = new ArrayList<UsageEvent>();
            for (int i = 0; i < 20; i++) {
                // A quarter of the events are late: they belong to a day up to five days back.
                int day = 10 - (rng.nextInt(4) == 0 ? rng.nextInt(6) : 0);
                var e = event("e" + id++, "acct-" + rng.nextInt(4), rng.nextInt(10_000), day);
                batch.add(e);
                truth.merge(new LineKey(e.accountId(), JAN1.plusDays(day)), e.computeMillis(), Long::sum);
            }
            table.append("p", seq, batch);
            if (seq % 3 == 0) {
                table.compact(100, 3);
            }
            snapshots.add(table.latestSnapshot());
        }
        Snapshot last = snapshots.getLast();
        assertEquals(truth, totals(billing.statement(last)));
        for (Snapshot from : snapshots) {
            var rebuilt = totals(billing.statement(from));
            billing.corrections(from, last).forEach(c ->
                    rebuilt.merge(new LineKey(c.accountId(), c.day()), c.computeMillis(), Long::sum));
            rebuilt.values().removeIf(v -> v == 0);
            assertEquals(truth, rebuilt, "invoice at v" + from.version() + " + corrections");
        }
    }

    @Test
    void arbitrarySqlRunsAgainstOldSnapshots() throws Exception {
        table.append("p", 1, TestEvents.batch("p", 1, 10));
        table.append("p", 2, TestEvents.batch("p", 2, 10));
        try (var q = SnapshotQuery.open(table, table.snapshotAt(1))) {
            assertEquals(10L, ((Number) q.query("SELECT count(*) AS n FROM usage").getFirst().get("n")).longValue());
        }
    }

    private record LineKey(String account, LocalDate day) {
    }

    private static Map<LineKey, Long> totals(List<LineItem> items) {
        var m = new HashMap<LineKey, Long>();
        items.forEach(i -> m.merge(new LineKey(i.accountId(), i.day()), i.computeMillis(), Long::sum));
        return m;
    }

    private static UsageEvent event(String id, String account, long millis, int dayOffset) {
        return new UsageEvent(id, account, "res", millis, TestEvents.T0 + dayOffset * TestEvents.DAY + 3_600_000);
    }
}
