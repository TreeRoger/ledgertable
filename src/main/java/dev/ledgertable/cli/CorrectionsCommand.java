package dev.ledgertable.cli;

import dev.ledgertable.billing.Billing;
import dev.ledgertable.core.LedgerTable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(name = "corrections", description = "Print the billing adjustments between two versions (e.g. from late events).")
final class CorrectionsCommand implements Callable<Integer> {
    @Mixin
    StoreOptions store;

    @Option(names = "--from", required = true, description = "Version that was invoiced.")
    long from;

    @Option(names = "--to", description = "Later version; defaults to the latest.")
    Long to;

    @Override
    public Integer call() throws Exception {
        try (var s = store.open()) {
            LedgerTable table = LedgerTable.open(s);
            var target = to == null ? table.latestSnapshot() : table.snapshotAt(to);
            System.out.println("-- corrections from version " + from + " to " + target.version());
            Tables.print(StatementCommand.rows(new Billing(table).corrections(table.snapshotAt(from), target)));
        }
        return 0;
    }
}
