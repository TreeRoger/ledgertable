package dev.ledgertable.log;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;

/** JSON encoding of log entries. Decoding is strict so that a corrupt entry fails loudly instead of being misread. */
public final class LogCodec {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES);

    private LogCodec() {
    }

    public static byte[] encode(Object entry) {
        try {
            return MAPPER.writeValueAsBytes(entry);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Commit decodeCommit(byte[] bytes) {
        return decode(bytes, Commit.class);
    }

    public static Checkpoint decodeCheckpoint(byte[] bytes) {
        return decode(bytes, Checkpoint.class);
    }

    private static <T> T decode(byte[] bytes, Class<T> type) {
        try {
            return MAPPER.readValue(bytes, type);
        } catch (IOException e) {
            throw new CorruptLogException("cannot decode " + type.getSimpleName(), e);
        }
    }
}
