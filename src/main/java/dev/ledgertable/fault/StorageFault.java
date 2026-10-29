package dev.ledgertable.fault;

/** What a {@link FaultInjectingObjectStore} does to one storage operation. */
public enum StorageFault {
    NONE,
    /** Throw an IOException without applying the operation. */
    ERROR_BEFORE,
    /** Apply the operation, then throw an IOException: the caller cannot tell whether it happened. */
    ERROR_AFTER,
    /** Kill the process before the operation reaches storage. */
    CRASH_BEFORE,
    /** Kill the process after the operation is durable. */
    CRASH_AFTER
}
