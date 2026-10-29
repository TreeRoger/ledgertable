package dev.ledgertable.harness;

import dev.ledgertable.core.UsageEvent;

import java.util.List;

/** One producer batch from a workload file. */
public record Batch(String producer, long seq, List<UsageEvent> events) {
}
