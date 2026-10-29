package dev.ledgertable.cli;

import dev.ledgertable.core.CompactionResult;
import dev.ledgertable.core.LedgerTable;
import dev.ledgertable.core.TableOptions;
import dev.ledgertable.fault.CrashPoint;
import dev.ledgertable.fault.Faults;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(name = "compact", description = "Compact small files, once or repeatedly for a while.")
final class CompactCommand implements Callable<Integer> {
    @Mixin
    StoreOptions store;

    @Option(names = "--small-file-rows", defaultValue = "1000")
    long smallFileRows;

    @Option(names = "--max-files", defaultValue = "8")
    int maxFiles;

    @Option(names = "--duration-seconds", defaultValue = "0", description = "Keep compacting this long; 0 compacts once.")
    double durationSeconds;

    @Option(names = "--pause-at", description = "Stop at this crash point and wait, so another process can kill this one.")
    CrashPoint pauseAt;

    @Option(names = "--checkpoint-interval", defaultValue = "10")
    int checkpointInterval;

    @Override
    public Integer call() throws Exception {
        Faults pause = point -> {
            if (point == pauseAt) {
                System.out.println("PAUSED compactor at " + point);
                System.out.flush();
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        long deadline = System.nanoTime() + (long) (durationSeconds * 1e9);
        try (var s = store.open()) {
            var table = LedgerTable.open(s, TableOptions.DEFAULT.withCheckpointInterval(checkpointInterval), pause);
            do {
                CompactionResult r = table.compact(smallFileRows, maxFiles);
                if (r.status() != CompactionResult.Status.NOTHING_TO_COMPACT) {
                    System.out.printf("%s v%d: %d files, %d rows%n", r.status(), r.version(), r.filesRemoved(), r.rows());
                } else {
                    Thread.sleep(50);
                }
            } while (System.nanoTime() < deadline);
        }
        return 0;
    }
}
