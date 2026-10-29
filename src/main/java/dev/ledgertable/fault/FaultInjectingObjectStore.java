package dev.ledgertable.fault;

import dev.ledgertable.store.ObjectStore;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/** Wraps a store and applies a {@link StoragePolicy} to every operation. */
public final class FaultInjectingObjectStore implements ObjectStore {

    /** Storage operations, as seen by a {@link StoragePolicy}. */
    public enum Op { PUT_IF_ABSENT, PUT, GET, LIST, DELETE }

    /** Chooses the fault for each operation; called once per operation in program order. */
    @FunctionalInterface
    public interface StoragePolicy {
        StorageFault decide(Op op, String key);
    }

    private final ObjectStore delegate;
    private final StoragePolicy policy;

    public FaultInjectingObjectStore(ObjectStore delegate, StoragePolicy policy) {
        this.delegate = delegate;
        this.policy = policy;
    }

    @Override
    public boolean putIfAbsent(String key, byte[] data) throws IOException {
        return apply(Op.PUT_IF_ABSENT, key, () -> delegate.putIfAbsent(key, data));
    }

    @Override
    public void put(String key, byte[] data) throws IOException {
        apply(Op.PUT, key, () -> {
            delegate.put(key, data);
            return null;
        });
    }

    @Override
    public Optional<byte[]> get(String key) throws IOException {
        return apply(Op.GET, key, () -> delegate.get(key));
    }

    @Override
    public List<String> list(String prefix, String startAfter) throws IOException {
        return apply(Op.LIST, prefix, () -> delegate.list(prefix, startAfter));
    }

    @Override
    public void delete(String key) throws IOException {
        apply(Op.DELETE, key, () -> {
            delegate.delete(key);
            return null;
        });
    }

    @Override
    public Optional<Path> localPath(String key) {
        return delegate.localPath(key);
    }

    private <T> T apply(Op op, String key, StorageCall<T> call) throws IOException {
        StorageFault fault = policy.decide(op, key);
        String where = op + " " + key;
        switch (fault) {
            case ERROR_BEFORE -> throw new IOException("injected error before " + where);
            case CRASH_BEFORE -> throw new SimulatedCrash("before " + where);
            default -> {
            }
        }
        T result = call.run();
        switch (fault) {
            case ERROR_AFTER -> throw new IOException("injected error after " + where);
            case CRASH_AFTER -> throw new SimulatedCrash("after " + where);
            default -> {
            }
        }
        return result;
    }

    @FunctionalInterface
    private interface StorageCall<T> {
        T run() throws IOException;
    }
}
