package dev.ledgertable.log;

import java.util.List;
import java.util.Map;

/** The full table state at {@code version}, stored as {@code _log/<version>.checkpoint.json}. */
public record Checkpoint(long version, long timestampMillis, List<AddFile> files, Map<String, ProducerState> producers) {
}
