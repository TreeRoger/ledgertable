package dev.ledgertable.core;

/** The writer lost every commit race it was allowed; nothing it wrote is referenced by the log. */
public final class CommitFailedException extends RuntimeException {
    public CommitFailedException(String message) {
        super(message);
    }
}
