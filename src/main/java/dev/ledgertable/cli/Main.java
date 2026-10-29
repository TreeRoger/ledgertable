package dev.ledgertable.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "ledger", mixinStandardHelpOptions = true, version = "ledgertable 0.1.0",
        description = "Transactional usage-metering table on object storage.",
        subcommands = {
                HarnessCommand.class, IngestCommand.class, CompactCommand.class, QueryCommand.class,
                StatementCommand.class, CorrectionsCommand.class, LogCommand.class, ExportCommand.class,
                BenchCommand.class
        })
public final class Main implements Runnable {
    public static void main(String[] args) {
        System.exit(new CommandLine(new Main()).execute(args));
    }

    @Override
    public void run() {
        new CommandLine(this).usage(System.out);
    }
}
