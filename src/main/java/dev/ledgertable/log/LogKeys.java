package dev.ledgertable.log;

import java.util.OptionalLong;

/** Object-key layout of the transaction log. Versions are zero-padded so lexicographic order is version order. */
public final class LogKeys {
    public static final String LOG_DIR = "_log/";
    /** Every commit and checkpoint key starts with this; the hint below does not. */
    public static final String VERSIONED_PREFIX = LOG_DIR + "0";
    public static final String CHECKPOINT_HINT = LOG_DIR + "_last_checkpoint";
    public static final String DATA_DIR = "data/";

    private static final String COMMIT_SUFFIX = ".json";
    private static final String CHECKPOINT_SUFFIX = ".checkpoint.json";

    private LogKeys() {
    }

    public static String versionPrefix(long version) {
        return LOG_DIR + String.format("%020d", version);
    }

    public static String commit(long version) {
        return versionPrefix(version) + COMMIT_SUFFIX;
    }

    public static String checkpoint(long version) {
        return versionPrefix(version) + CHECKPOINT_SUFFIX;
    }

    public static OptionalLong parseCommit(String key) {
        return parse(key, COMMIT_SUFFIX);
    }

    public static OptionalLong parseCheckpoint(String key) {
        return parse(key, CHECKPOINT_SUFFIX);
    }

    private static OptionalLong parse(String key, String suffix) {
        int digitsEnd = LOG_DIR.length() + 20;
        if (!key.startsWith(LOG_DIR) || key.length() != digitsEnd + suffix.length() || !key.endsWith(suffix)) {
            return OptionalLong.empty();
        }
        try {
            return OptionalLong.of(Long.parseLong(key.substring(LOG_DIR.length(), digitsEnd)));
        } catch (NumberFormatException e) {
            return OptionalLong.empty();
        }
    }
}
