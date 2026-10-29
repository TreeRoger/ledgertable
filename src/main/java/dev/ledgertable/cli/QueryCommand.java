package dev.ledgertable.cli;

import dev.ledgertable.core.LedgerTable;
import dev.ledgertable.core.Snapshot;
import dev.ledgertable.query.SnapshotQuery;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;

import java.util.concurrent.Callable;

@Command(name = "query", description = "Run DuckDB SQL against a snapshot, exposed as the view `usage`.")
final class QueryCommand implements Callable<Integer> {
    @Mixin
    StoreOptions store;

    @Mixin
    SnapshotChoice snapshot;

    @Parameters(index = "0", description = "SQL, e.g. \"SELECT account_id, SUM(compute_millis) FROM usage GROUP BY 1\"")
    String sql;

    @Override
    public Integer call() throws Exception {
        try (var s = store.open()) {
            LedgerTable table = LedgerTable.open(s);
            Snapshot snap = snapshot.resolve(table);
            System.out.println("-- version " + snap.version());
            try (var q = SnapshotQuery.open(table, snap)) {
                Tables.print(q.query(sql));
            }
        }
        return 0;
    }
}
