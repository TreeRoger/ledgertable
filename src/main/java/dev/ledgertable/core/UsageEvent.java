package dev.ledgertable.core;

import java.util.Objects;

/**
 * One metered usage record. Compute is stored as integer milliseconds so that billing
 * totals are exact sums rather than floating-point approximations.
 */
public record UsageEvent(String eventId, String accountId, String resourceId, long computeMillis, long eventTimeMillis) {
    public UsageEvent {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(resourceId, "resourceId");
        if (computeMillis < 0) {
            throw new IllegalArgumentException("computeMillis must be non-negative: " + computeMillis);
        }
    }
}
