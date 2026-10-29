package dev.ledgertable;

import dev.ledgertable.core.UsageEvent;

import java.util.ArrayList;
import java.util.List;

public final class TestEvents {
    public static final long DAY = 86_400_000L;
    public static final long T0 = 1_767_225_600_000L; // 2026-01-01T00:00:00Z

    private TestEvents() {
    }

    /** {@code n} events for batch {@code seq} of {@code producer}, with ids unique across producers and batches. */
    public static List<UsageEvent> batch(String producer, long seq, int n) {
        var events = new ArrayList<UsageEvent>(n);
        for (int i = 0; i < n; i++) {
            events.add(new UsageEvent(producer + "-" + seq + "-" + i, "acct-" + (i % 5), "res-" + (i % 3),
                    1000L + i, T0 + seq * 60_000L + i));
        }
        return events;
    }
}
