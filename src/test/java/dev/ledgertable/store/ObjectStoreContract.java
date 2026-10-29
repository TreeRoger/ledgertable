package dev.ledgertable.store;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Behaviour every {@link ObjectStore} implementation must provide. */
abstract class ObjectStoreContract {
    protected ObjectStore store;

    protected abstract ObjectStore createStore() throws Exception;

    @BeforeEach
    void setUp() throws Exception {
        store = createStore();
    }

    @AfterEach
    void tearDown() {
        store.close();
    }

    @Test
    void putIfAbsentCreatesOnlyOnce() throws Exception {
        assertTrue(store.putIfAbsent("a/key", bytes("first")));
        assertFalse(store.putIfAbsent("a/key", bytes("second")));
        assertArrayEquals(bytes("first"), store.get("a/key").orElseThrow());
    }

    @Test
    void getMissingKeyIsEmpty() throws Exception {
        assertTrue(store.get("missing").isEmpty());
    }

    @Test
    void putReplaces() throws Exception {
        store.put("hint", bytes("1"));
        store.put("hint", bytes("2"));
        assertArrayEquals(bytes("2"), store.get("hint").orElseThrow());
    }

    @Test
    void listIsSortedAndHonoursStartAfter() throws Exception {
        for (String k : List.of("log/003", "log/001", "log/002", "other/001")) {
            store.putIfAbsent(k, bytes(k));
        }
        assertEquals(List.of("log/001", "log/002", "log/003"), store.list("log/"));
        assertEquals(List.of("log/002", "log/003"), store.list("log/", "log/001"));
        assertEquals(List.of("log/003"), store.list("log/00", "log/002"));
    }

    @Test
    void deleteRemovesAndIsIdempotent() throws Exception {
        store.putIfAbsent("gone", bytes("x"));
        store.delete("gone");
        store.delete("gone");
        assertTrue(store.get("gone").isEmpty());
    }

    @Test
    void concurrentPutIfAbsentHasExactlyOneWinnerPerKey() throws Exception {
        int threads = 16;
        int keys = 25;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int k = 0; k < keys; k++) {
                String key = "race/" + k;
                var start = new CountDownLatch(1);
                var futures = new ArrayList<Future<Boolean>>();
                for (int t = 0; t < threads; t++) {
                    byte[] payload = bytes("writer-" + t);
                    futures.add(pool.submit(() -> {
                        start.await();
                        return store.putIfAbsent(key, payload);
                    }));
                }
                start.countDown();
                int winners = 0;
                int winner = -1;
                for (int t = 0; t < threads; t++) {
                    if (futures.get(t).get()) {
                        winners++;
                        winner = t;
                    }
                }
                assertEquals(1, winners, "winners for " + key);
                assertArrayEquals(bytes("writer-" + winner), store.get(key).orElseThrow());
            }
        } finally {
            pool.shutdownNow();
        }
    }

    protected static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
