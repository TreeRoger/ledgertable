package dev.ledgertable.store;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    void rejectsKeysEscapingTheRoot() {
        assertThrows(IllegalArgumentException.class, () -> store.get("../outside"));
    }
}
