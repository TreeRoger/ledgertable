package dev.ledgertable.cli;

import dev.ledgertable.harness.FaultPlan;
import dev.ledgertable.harness.Harness;
import dev.ledgertable.harness.History;
import dev.ledgertable.harness.Workload;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.util.concurrent.Callable;

@Command(name = "harness", description = "Run a workload with concurrent faulty clients and record a history.")
final class HarnessCommand implements Callable<Integer> {
    @Mixin
    StoreOptions store;

    @Option(names = "--workload", required = true)
    Path workload;

    @Option(names = "--history", required = true)
    Path history;

    @Option(names = "--seed", defaultValue = "1")
    long seed;

    @Option(names = "--compactors", defaultValue = "2")
    int compactors;

    @Option(names = "--readers", defaultValue = "2")
    int readers;

    @Option(names = "--crash-rate", defaultValue = "0.02", description = "Chance of dying at each crash point and storage op.")
    double crashRate;

    @Option(names = "--error-rate", defaultValue = "0.02", description = "Chance that a storage op fails.")
    double errorRate;

    @Option(names = "--checkpoint-interval", defaultValue = "10")
    int checkpointInterval;

    @Option(names = "--small-file-rows", defaultValue = "200")
    long smallFileRows;

    @Option(names = "--max-compaction-files", defaultValue = "6")
    int maxCompactionFiles;

    @Option(names = "--max-batch-pause-ms", defaultValue = "100",
            description = "Each producer waits a random 0..N ms between batches, so the run lasts long enough for compactors and readers to overlap with it.")
    int maxBatchPauseMillis;

    @Override
    public Integer call() throws Exception {
        var config = new Harness.Config(seed, compactors, readers, new FaultPlan(crashRate, errorRate),
                checkpointInterval, smallFileRows, maxCompactionFiles, maxBatchPauseMillis);
        try (var s = store.open(); var h = new History(history)) {
            return new Harness(s, config, h).run(Workload.load(workload)) ? 0 : 2;
        }
    }
}
