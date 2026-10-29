package dev.ledgertable.core;

import java.util.function.LongSupplier;

/**
 * @param checkpointInterval write a checkpoint at every version divisible by this; 0 disables checkpoints
 * @param maxCommitAttempts  give up after losing this many commit races in a row
 * @param maxBackoffMicros   cap of the randomized exponential backoff after a lost race
 * @param clock              wall clock for commit timestamps
 */
public record TableOptions(int checkpointInterval, int maxCommitAttempts, long maxBackoffMicros, LongSupplier clock) {
    public static final TableOptions DEFAULT = new TableOptions(25, 10_000, 5_000, System::currentTimeMillis);

    public TableOptions withCheckpointInterval(int interval) {
        return new TableOptions(interval, maxCommitAttempts, maxBackoffMicros, clock);
    }

    public TableOptions withMaxBackoffMicros(long micros) {
        return new TableOptions(checkpointInterval, maxCommitAttempts, micros, clock);
    }

    public TableOptions withClock(LongSupplier newClock) {
        return new TableOptions(checkpointInterval, maxCommitAttempts, maxBackoffMicros, newClock);
    }
}
