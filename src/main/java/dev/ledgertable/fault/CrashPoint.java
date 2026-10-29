package dev.ledgertable.fault;

/** Named points in the write path where a process can be killed. */
public enum CrashPoint {
    BEFORE_DATA_WRITE,
    AFTER_DATA_WRITE,
    COMPACTION_AFTER_READ,
    BEFORE_COMMIT_WRITE,
    /** The commit object is durable but the writer has not yet acknowledged it. */
    AFTER_COMMIT_WRITE,
    AFTER_CONFLICT_DETECTED,
    BEFORE_CHECKPOINT_WRITE,
    AFTER_CHECKPOINT_WRITE
}
