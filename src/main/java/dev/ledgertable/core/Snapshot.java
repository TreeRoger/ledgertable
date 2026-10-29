package dev.ledgertable.core;

import dev.ledgertable.log.AddFile;
import dev.ledgertable.log.Checkpoint;
import dev.ledgertable.log.Commit;
import dev.ledgertable.log.CorruptLogException;
import dev.ledgertable.log.Operation;
import dev.ledgertable.log.ProducerState;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Immutable table state at one version: the live data files and, per producer, the newest
 * committed batch. Built by replaying commits in order; every replay step re-checks the
 * invariants a correct writer maintains, so a bad log is detected rather than silently read.
 */
public final class Snapshot {
    /** The state before version 0 exists. */
    static final Snapshot EMPTY = new Snapshot(-1, Long.MIN_VALUE, Map.of(), Map.of());

    private final long version;
    private final long timestampMillis;
    private final Map<String, AddFile> files;
    private final Map<String, ProducerState> producers;

    private Snapshot(long version, long timestampMillis, Map<String, AddFile> files, Map<String, ProducerState> producers) {
        this.version = version;
        this.timestampMillis = timestampMillis;
        this.files = Collections.unmodifiableMap(files);
        this.producers = Collections.unmodifiableMap(producers);
    }

    static Snapshot fromCheckpoint(Checkpoint cp) {
        var files = new LinkedHashMap<String, AddFile>();
        cp.files().forEach(f -> files.put(f.path(), f));
        return new Snapshot(cp.version(), cp.timestampMillis(), files, new LinkedHashMap<>(cp.producers()));
    }

    public long version() {
        return version;
    }

    public long timestampMillis() {
        return timestampMillis;
    }

    public Collection<AddFile> files() {
        return files.values();
    }

    public boolean contains(String path) {
        return files.containsKey(path);
    }

    public Map<String, ProducerState> producers() {
        return producers;
    }

    public Optional<ProducerState> producer(String producerId) {
        return Optional.ofNullable(producers.get(producerId));
    }

    public long rowCount() {
        return files.values().stream().mapToLong(AddFile::rows).sum();
    }

    public long computeMillis() {
        return files.values().stream().mapToLong(AddFile::computeMillis).sum();
    }

    public Checkpoint toCheckpoint() {
        return new Checkpoint(version, timestampMillis, List.copyOf(files.values()), Map.copyOf(producers));
    }

    Snapshot apply(Commit commit) {
        return new Builder(this).apply(commit).build();
    }

    /** Mutable replay state, so replaying a long log does not copy the file map once per commit. */
    static final class Builder {
        private long version;
        private long timestampMillis;
        private final LinkedHashMap<String, AddFile> files;
        private final LinkedHashMap<String, ProducerState> producers;

        Builder(Snapshot base) {
            version = base.version;
            timestampMillis = base.timestampMillis;
            files = new LinkedHashMap<>(base.files);
            producers = new LinkedHashMap<>(base.producers);
        }

        Builder apply(Commit c) {
            check(c.version() == version + 1, c, "expected version " + (version + 1));
            check(c.timestampMillis() > timestampMillis, c, "timestamp does not increase");
            check((c.operation() == Operation.CREATE) == (c.version() == 0), c, "CREATE must be exactly version 0");
            check(c.readVersion() < c.version(), c, "readVersion must precede version");
            for (String path : c.removes()) {
                check(files.remove(path) != null, c, "removes missing file " + path);
            }
            for (AddFile add : c.adds()) {
                check(files.putIfAbsent(add.path(), add) == null, c, "adds live file " + add.path());
            }
            if (c.producer() != null) {
                ProducerState prev = producers.get(c.producer().producerId());
                long expected = prev == null ? 1 : prev.seq() + 1;
                check(c.producer().seq() == expected, c, "producer " + c.producer().producerId() + " expected seq " + expected);
                producers.put(c.producer().producerId(), new ProducerState(c.producer().seq(), c.version()));
            }
            version = c.version();
            timestampMillis = c.timestampMillis();
            return this;
        }

        Snapshot build() {
            return new Snapshot(version, timestampMillis, new LinkedHashMap<>(files), new LinkedHashMap<>(producers));
        }

        private static void check(boolean ok, Commit c, String what) {
            if (!ok) {
                throw new CorruptLogException("commit " + c.version() + ": " + what);
            }
        }
    }
}
