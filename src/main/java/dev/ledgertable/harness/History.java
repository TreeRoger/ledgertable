package dev.ledgertable.harness;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Append-only JSONL log of what clients asked for and what they were told, in the spirit of a
 * Jepsen history. Each line is flushed as it is written so a killed process loses at most a
 * partial final line, which the checker ignores. {@code t} is wall-clock nanoseconds, so
 * histories written by different processes on one machine can be merged and ordered.
 */
public final class History implements AutoCloseable {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final BufferedWriter out;

    public History(Path path) throws IOException {
        out = Files.newBufferedWriter(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public static long now() {
        var instant = java.time.Instant.now();
        return instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
    }

    /** Records an entry; {@code fields} alternate between names and values. */
    public void record(String type, Object... fields) {
        var entry = new LinkedHashMap<String, Object>();
        entry.put("type", type);
        entry.put("t", now());
        for (int i = 0; i < fields.length; i += 2) {
            entry.put((String) fields[i], fields[i + 1]);
        }
        write(entry);
    }

    private synchronized void write(Map<String, Object> entry) {
        try {
            out.write(MAPPER.writeValueAsString(entry));
            out.newLine();
            out.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        out.close();
    }
}
