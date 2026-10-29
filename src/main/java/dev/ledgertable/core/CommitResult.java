package dev.ledgertable.core;

/**
 * Acknowledgement of an append.
 *
 * @param version   a version whose snapshot contains the batch; for a fresh commit, the version it created
 * @param duplicate true if the batch had already been committed and this call changed nothing
 * @param attempts  commit races this call took part in (1 when uncontended; 0 for a duplicate found before writing)
 */
public record CommitResult(long version, boolean duplicate, int attempts) {
}
