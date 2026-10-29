package dev.ledgertable.log;

/** The newest committed batch of a producer and the table version that committed it. */
public record ProducerState(long seq, long version) {
}
