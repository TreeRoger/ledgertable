package dev.ledgertable.harness;

import dev.ledgertable.core.CommitFailedException;
import dev.ledgertable.core.CommitResult;
import dev.ledgertable.core.LedgerTable;
import dev.ledgertable.core.TableOptions;
import dev.ledgertable.fault.Faults;
import dev.ledgertable.fault.SimulatedCrash;
import dev.ledgertable.log.ProducerState;
import dev.ledgertable.store.ObjectStore;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * Drives one producer through its batches the way a real ingestion client would: on an error it
 * resubmits the same batch, and when its process dies it restarts, asks the table which batch
 * committed last, and carries on from the next one.
 */
public final class ProducerLoop {
    private final ObjectStore store;
    private final TableOptions options;
    private final FaultPlan plan;
    private final long seed;
    private final History history;
    private final String producer;
    private final List<Batch> batches;
    private final int maxBatchPauseMillis;
    private final Faults extraFaults;

    /**
     * @param maxBatchPauseMillis upper bound of the random pause between batches, which stands in for the time a real producer spends collecting its next batch
     * @param extraFaults crash-point hook applied on top of the random plan (the demo uses it to pause a writer mid-commit)
     */
    public ProducerLoop(ObjectStore store, TableOptions options, FaultPlan plan, long seed, History history,
                        String producer, List<Batch> batches, int maxBatchPauseMillis, Faults extraFaults) {
        this.store = store;
        this.options = options;
        this.plan = plan;
        this.seed = seed;
        this.history = history;
        this.producer = producer;
        this.batches = batches;
        this.maxBatchPauseMillis = maxBatchPauseMillis;
        this.extraFaults = extraFaults;
    }

    public void run() throws InterruptedException {
        var rng = new Random(seed);
        int next = -1;
        for (int incarnation = 1; next < batches.size(); incarnation++) {
            String process = producer + "#" + incarnation;
            FaultPlan.Process faults = plan.incarnation(seed * 31 + incarnation);
            Faults crashPoints = faults.crashPoints();
            Faults hooks = point -> {
                extraFaults.at(point);
                crashPoints.at(point);
            };
            try {
                LedgerTable table = LedgerTable.open(faults.wrap(store), options, hooks);
                long committed = table.latestSnapshot().producer(producer).map(ProducerState::seq).orElse(0L);
                history.record("recover", "process", process, "producer", producer, "seq", committed);
                next = (int) committed;
                while (next < batches.size()) {
                    Batch batch = batches.get(next);
                    history.record("invoke", "op", "append", "process", process, "producer", producer, "seq", batch.seq());
                    try {
                        CommitResult r = table.append(producer, batch.seq(), batch.events());
                        history.record("ok", "op", "append", "process", process, "producer", producer, "seq", batch.seq(),
                                "version", r.version(), "duplicate", r.duplicate(), "attempts", r.attempts());
                        next++;
                        if (maxBatchPauseMillis > 0) {
                            Thread.sleep(rng.nextInt(maxBatchPauseMillis + 1));
                        }
                    } catch (IOException | CommitFailedException e) {
                        history.record("info", "op", "append", "process", process, "producer", producer, "seq", batch.seq(),
                                "error", e.toString());
                    }
                }
            } catch (SimulatedCrash crash) {
                history.record("crash", "process", process, "producer", producer, "error", crash.getMessage());
            } catch (IOException e) {
                history.record("crash", "process", process, "producer", producer, "error", "failed to start: " + e);
            }
        }
    }
}
