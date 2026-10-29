package dev.ledgertable.billing;

import java.time.LocalDate;

/** Compute billed to one account for one UTC day; for a correction, the signed change to that amount. */
public record LineItem(String accountId, LocalDate day, long computeMillis) {
}
