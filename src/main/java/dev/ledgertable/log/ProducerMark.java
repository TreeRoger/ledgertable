package dev.ledgertable.log;

/** Records that a commit carries batch {@code seq} of producer {@code producerId}. */
public record ProducerMark(String producerId, long seq) {
}
