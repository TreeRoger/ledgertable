package dev.ledgertable.cli;

import dev.ledgertable.billing.Billing;
import dev.ledgertable.billing.LineItem;
import dev.ledgertable.core.LedgerTable;
import dev.ledgertable.core.Snapshot;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@Command(name = "statement", description = "Print the billing statement (compute per account and UTC day) for a snapshot.")
final class StatementCommand implements Callable<Integer> {
    @Mixin
    StoreOptions store;

    @Mixin
    SnapshotChoice snapshot;

    @Override
    public Integer call() throws Exception {
        try (var s = store.open()) {
            LedgerTable table = LedgerTable.open(s);
            Snapshot snap = snapshot.resolve(table);
            System.out.println("-- statement at version " + snap.version());
            Tables.print(rows(new Billing(table).statement(snap)));
        }
        return 0;
    }

    static List<Map<String, Object>> rows(List<LineItem> items) {
        return items.stream().map(i -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("account_id", i.accountId());
            row.put("day", i.day());
            row.put("compute_seconds", String.format("%d.%03d", i.computeMillis() / 1000, Math.abs(i.computeMillis() % 1000)));
            return row;
        }).toList();
    }
}
