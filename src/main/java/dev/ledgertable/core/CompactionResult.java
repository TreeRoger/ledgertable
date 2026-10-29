package dev.ledgertable.core;

public record CompactionResult(Status status, long version, int filesRemoved, long rows) {
    public enum Status { COMPACTED, NOTHING_TO_COMPACT, CONFLICT }

    static CompactionResult nothing(long version) {
        return new CompactionResult(Status.NOTHING_TO_COMPACT, version, 0, 0);
    }
}
