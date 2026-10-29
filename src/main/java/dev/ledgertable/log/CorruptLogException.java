package dev.ledgertable.log;

/** The log violates an invariant that a correct writer can never break. */
public final class CorruptLogException extends RuntimeException {
    public CorruptLogException(String message) {
        super(message);
    }

    public CorruptLogException(String message, Throwable cause) {
        super(message, cause);
    }
}
