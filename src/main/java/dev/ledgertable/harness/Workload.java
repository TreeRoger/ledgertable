package dev.ledgertable.harness;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.ledgertable.core.UsageEvent;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads a workload file written by {@code py/ledger/workload.py}: one JSON batch per line,
 * {@code {"producer": ..., "seq": ..., "events": [[event_id, account_id, resource_id, compute_millis, event_time], ...]}}.
 */
public final class Workload {
    private Workload() {
    }

    /** Batches grouped by producer, each producer's batches in sequence order. */
    public static Map<String, List<Batch>> load(Path path) throws IOException {
        var mapper = new ObjectMapper();
        var byProducer = new LinkedHashMap<String, List<Batch>>();
        try (BufferedReader in = Files.newBufferedReader(path)) {
            for (String line = in.readLine(); line != null; line = in.readLine()) {
                if (line.isBlank()) {
                    continue;
                }
                JsonNode node = mapper.readTree(line);
                var events = new ArrayList<UsageEvent>();
                for (JsonNode e : node.get("events")) {
                    events.add(new UsageEvent(e.get(0).asText(), e.get(1).asText(), e.get(2).asText(),
                            e.get(3).asLong(), e.get(4).asLong()));
                }
                var batch = new Batch(node.get("producer").asText(), node.get("seq").asLong(), events);
                List<Batch> batches = byProducer.computeIfAbsent(batch.producer(), p -> new ArrayList<>());
                if (batch.seq() != batches.size() + 1) {
                    throw new IOException("producer " + batch.producer() + " batches must be numbered 1, 2, ... in order");
                }
                batches.add(batch);
            }
        }
        return byProducer;
    }
}
