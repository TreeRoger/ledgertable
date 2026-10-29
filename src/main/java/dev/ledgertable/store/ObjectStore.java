package dev.ledgertable.store;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/**
 * Minimal object-storage contract the table layer relies on. Every operation is either fully
 * applied or not applied at all; an {@link IOException} means the outcome is unknown to the caller.
 */
public interface ObjectStore extends AutoCloseable {

    /**
     * Atomically creates {@code key} with {@code data} only if no object exists under that key.
     * Readers never observe a partially written object.
     *
     * @return true if this call created the object, false if the key already existed
     */
    boolean putIfAbsent(String key, byte[] data) throws IOException;

    /** Creates or replaces {@code key}. Used only for advisory hints, never for table state. */
    void put(String key, byte[] data) throws IOException;

    Optional<byte[]> get(String key) throws IOException;

    /** Returns all keys starting with {@code prefix} that sort strictly after {@code startAfter}, in lexicographic order. */
    List<String> list(String prefix, String startAfter) throws IOException;

    default List<String> list(String prefix) throws IOException {
        return list(prefix, "");
    }

    void delete(String key) throws IOException;

    /** A local filesystem path for {@code key}, when the store is backed by one (lets DuckDB read files in place). */
    default Optional<Path> localPath(String key) {
        return Optional.empty();
    }

    @Override
    default void close() {
    }
}
