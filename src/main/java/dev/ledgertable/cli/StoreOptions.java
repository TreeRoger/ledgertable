package dev.ledgertable.cli;

import dev.ledgertable.store.LocalObjectStore;
import dev.ledgertable.store.ObjectStore;
import dev.ledgertable.store.S3Config;
import dev.ledgertable.store.S3ObjectStore;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Path;

/** Mixin: where the table lives: a local directory, or a key prefix on the S3 endpoint given by {@code LEDGER_S3_*}. */
final class StoreOptions {
    @ArgGroup(exclusive = true, multiplicity = "1")
    Location location;

    static final class Location {
        @Option(names = "--table", description = "Table directory on the local filesystem.")
        Path dir;

        @Option(names = "--s3-prefix", description = "Table key prefix on the S3 endpoint from LEDGER_S3_ENDPOINT.")
        String s3Prefix;
    }

    ObjectStore open() throws IOException {
        if (location.dir != null) {
            return new LocalObjectStore(location.dir);
        }
        S3Config config = S3Config.fromEnv(location.s3Prefix);
        if (config == null) {
            throw new IllegalArgumentException("--s3-prefix needs LEDGER_S3_ENDPOINT");
        }
        return S3ObjectStore.connect(config);
    }
}
