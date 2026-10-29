package dev.ledgertable.cli;

import dev.ledgertable.core.TableOptions;
import dev.ledgertable.fault.CrashPoint;
import dev.ledgertable.fault.Faults;
import dev.ledgertable.harness.FaultPlan;
import dev.ledgertable.harness.History;
import dev.ledgertable.harness.ProducerLoop;
import dev.ledgertable.harness.Workload;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

@Command(name = "ingest", description = "Ingest one producer's batches from a workload file, resuming after the last committed batch.")
final class IngestCommand implements Callable<Integer> {
    @Mixin
    StoreOptions store;

    @Option(names = "--workload", required = true)
    Path workload;

    @Option(names = "--producer", required = true)
    String producer;

    @Option(names = "--history", required = true)
    Path history;

    @Option(names = "--seed", defaultValue = "1")
    long seed;

    @Option(names = "--checkpoint-interval", defaultValue = "10")
    int checkpointInterval;

    @Option(names = "--pause-at", description = "Stop at this crash point and wait, so another process can kill this one.")
    CrashPoint pauseAt;

    @Option(names = "--pause-occurrence", defaultValue = "1", description = "Pause at the n-th time --pause-at is reached.")
    int pauseOccurrence;

    @Option(names = "--pause-millis", defaultValue = "60000")
    long pauseMillis;

    @Override
    public Integer call() throws Exception {
        var batches = Workload.load(workload).get(producer);
        if (batches == null) {
            throw new IllegalArgumentException("no batches for producer " + producer);
        }
        var reached = new AtomicInteger();
        Faults pause = point -> {
            if (point == pauseAt && reached.incrementAndGet() == pauseOccurrence) {
                System.out.println("PAUSED " + producer + " at " + point);
                System.out.flush();
                try {
                    Thread.sleep(pauseMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        var options = TableOptions.DEFAULT.withCheckpointInterval(checkpointInterval);
        try (var s = store.open(); var h = new History(history)) {
            new ProducerLoop(s, options, FaultPlan.NONE, seed, h, producer, batches, 0, pause).run();
        }
        System.out.println("DONE " + producer);
        return 0;
    }
}
