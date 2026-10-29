package dev.ledgertable.cli;

import dev.ledgertable.core.LedgerTable;
import dev.ledgertable.core.Snapshot;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.time.Instant;

/** Mixin: which snapshot to read, defaulting to the latest. */
final class SnapshotChoice {
    @ArgGroup(exclusive = true)
    Choice choice;

    static final class Choice {
        @Option(names = "--version", description = "Read the table as of this version.")
        Long version;

        @Option(names = "--as-of", description = "Read the table as of this instant (ISO-8601, e.g. 2026-01-01T00:00:00Z).")
        Instant asOf;
    }

    Snapshot resolve(LedgerTable table) throws IOException {
        if (choice == null) {
            return table.latestSnapshot();
        }
        if (choice.version != null) {
            return table.snapshotAt(choice.version);
        }
        return table.snapshotAsOf(choice.asOf.toEpochMilli());
    }
}
