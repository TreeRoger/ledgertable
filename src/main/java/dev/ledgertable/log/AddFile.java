package dev.ledgertable.log;

/** A data file that a commit adds to the table, with the statistics billing and compaction need without opening it. */
public record AddFile(String path, long rows, long bytes, long computeMillis, long minEventTime, long maxEventTime) {
}
