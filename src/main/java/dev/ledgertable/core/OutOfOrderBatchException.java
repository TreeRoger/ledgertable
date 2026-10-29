package dev.ledgertable.core;

/** A producer submitted a batch whose sequence number skips ahead of its last committed batch. */
public final class OutOfOrderBatchException extends IllegalStateException {
    public OutOfOrderBatchException(String producerId, long expected, long got) {
        super("producer " + producerId + " must submit seq " + expected + " next, got " + got);
    }
}
