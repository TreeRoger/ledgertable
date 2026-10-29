package dev.ledgertable.io;

import dev.ledgertable.core.UsageEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ParquetCodecTest {
    @Test
    void roundTripsEvents() {
        var events = new ArrayList<UsageEvent>();
        for (int i = 0; i < 1000; i++) {
            events.add(new UsageEvent("e" + i, "acct-" + (i % 7), "cluster-" + (i % 3), i * 17L, 1_760_000_000_000L + i));
        }
        assertEquals(events, ParquetCodec.decode(ParquetCodec.encode(events)));
    }

    @Test
    void roundTripsEmptyFile() {
        assertEquals(List.of(), ParquetCodec.decode(ParquetCodec.encode(List.of())));
    }
}
