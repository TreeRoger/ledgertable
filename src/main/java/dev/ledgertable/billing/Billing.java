package dev.ledgertable.billing;

import dev.ledgertable.core.LedgerTable;
import dev.ledgertable.core.Snapshot;
import dev.ledgertable.log.AddFile;
import dev.ledgertable.query.SnapshotQuery;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Billing over snapshots. A statement is the per-account, per-day total at one version.
 * Events may arrive days late, landing in a day that was already invoiced; {@link #corrections}
 * gives the exact adjustment between two versions by reading only the files that differ.
 */
public final class Billing {
    /** The billing SQL: sum compute per account and UTC event day. */
    public static final String STATEMENT_SQL = """
            SELECT account_id,
                   (epoch_ms(event_time) // 86400000)::BIGINT AS day,
                   SUM(compute_millis)::BIGINT AS compute_millis
            FROM %s
            GROUP BY ALL
            ORDER BY account_id, day""";

    private final LedgerTable table;

    public Billing(LedgerTable table) {
        this.table = table;
    }

    public List<LineItem> statement(Snapshot snapshot) throws IOException, SQLException {
        try (var q = SnapshotQuery.open(table, snapshot)) {
            return lineItems(q.query(STATEMENT_SQL.formatted(SnapshotQuery.VIEW)));
        }
    }

    /**
     * Signed per-account, per-day changes that turn the statement at {@code from} into the
     * statement at {@code to}. Compaction rewrites files without changing rows, so its added and
     * removed files cancel out and produce no line items.
     */
    public List<LineItem> corrections(Snapshot from, Snapshot to) throws IOException, SQLException {
        List<AddFile> added = to.files().stream().filter(f -> !from.contains(f.path())).toList();
        List<AddFile> removed = from.files().stream().filter(f -> !to.contains(f.path())).toList();
        try (var q = SnapshotQuery.empty(table)) {
            q.createView("added", added);
            q.createView("removed", removed);
            return lineItems(q.query("""
                    SELECT account_id,
                           (epoch_ms(event_time) // 86400000)::BIGINT AS day,
                           SUM(compute_millis * sign)::BIGINT AS compute_millis
                    FROM (SELECT *, 1 AS sign FROM added UNION ALL SELECT *, -1 AS sign FROM removed)
                    GROUP BY ALL
                    HAVING SUM(compute_millis * sign) <> 0
                    ORDER BY account_id, day"""));
        }
    }

    private static List<LineItem> lineItems(List<Map<String, Object>> rows) {
        var items = new ArrayList<LineItem>(rows.size());
        for (var row : rows) {
            items.add(new LineItem((String) row.get("account_id"),
                    LocalDate.ofEpochDay(((Number) row.get("day")).longValue()),
                    ((Number) row.get("compute_millis")).longValue()));
        }
        return items;
    }
}
