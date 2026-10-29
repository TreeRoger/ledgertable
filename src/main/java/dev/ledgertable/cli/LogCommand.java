package dev.ledgertable.cli;

import dev.ledgertable.core.LedgerTable;
import dev.ledgertable.log.Commit;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;

@Command(name = "log", description = "Print the transaction log.")
final class LogCommand implements Callable<Integer> {
    @Mixin
    StoreOptions store;

    @Override
    public Integer call() throws Exception {
        try (var s = store.open()) {
            LedgerTable table = LedgerTable.open(s);
            long latest = table.latestSnapshot().version();
            var rows = new ArrayList<Map<String, Object>>();
            for (long v = 0; v <= latest; v++) {
                Commit c = table.readCommit(v).orElseThrow();
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("version", c.version());
                row.put("committed_at", Instant.ofEpochMilli(c.timestampMillis()));
                row.put("operation", c.operation());
                row.put("read_version", c.readVersion());
                row.put("adds", c.adds().size());
                row.put("removes", c.removes().size());
                row.put("rows_added", c.adds().stream().mapToLong(a -> a.rows()).sum());
                row.put("batch", c.producer() == null ? "" : c.producer().producerId() + "/" + c.producer().seq());
                rows.add(row);
            }
            Tables.print(rows);
        }
        return 0;
    }
}
