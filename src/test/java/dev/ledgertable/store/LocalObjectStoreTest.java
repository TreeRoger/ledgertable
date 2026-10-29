package dev.ledgertable.store;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalObjectStoreTest extends ObjectStoreContract {
    @TempDir
    Path dir;

    @Override
    protected ObjectStore createStore() throws Exception {
        return new LocalObjectStore(dir);
    }

    @Test
    void stagingFilesAreNeverListedOrLeftBehind() throws Exception {
        store.putIfAbsent("k", bytes("v"));
        store.putIfAbsent("k", bytes("v2"));
        assertEquals(List.of("k"), store.list(""));
        try (var staged = Files.list(dir.resolve(".tmp"))) {
            assertEquals(0, staged.count());
        }
    }

    @Test
    void putIfAbsentSyncsContentsBeforePublishingAndTheDirectoryAfter() throws Exception {
        var events = new ArrayList<String>();
        Path target = dir.resolve("synced/_log/00000000000000000001.json");
        var recording = new LocalObjectStore.Sync() {
            @Override
            public void file(FileChannel channel) throws IOException {
                events.add("file, published=" + Files.exists(target));
                LocalObjectStore.Sync.FSYNC.file(channel);
            }

            @Override
            public void directory(Path d) throws IOException {
                events.add("dir " + dir.relativize(d) + ", published=" + Files.exists(target));
                LocalObjectStore.Sync.FSYNC.directory(d);
            }
        };
        var synced = new LocalObjectStore(dir.resolve("synced"), recording);
        events.clear();
        assertTrue(synced.putIfAbsent("_log/00000000000000000001.json", bytes("commit")));
        assertEquals(List.of(
                "dir synced, published=false",
                "file, published=false",
                "dir synced/_log, published=true"), events);
        events.clear();
        assertFalse(synced.putIfAbsent("_log/00000000000000000001.json", bytes("loser")));
        assertEquals(List.of("file, published=true"), events, "a losing put publishes nothing, so syncs no directory");
    }

    @Test
    void rejectsKeysEscapingTheRoot() {
        assertThrows(IllegalArgumentException.class, () -> store.get("../outside"));
    }
}
