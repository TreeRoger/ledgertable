package dev.ledgertable.cli;

import dev.ledgertable.bench.Bench;
import dev.ledgertable.store.LocalObjectStore;
import dev.ledgertable.store.S3Config;
import dev.ledgertable.store.S3ObjectStore;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.Callable;

@Command(name = "bench", description = "Run the benchmarks and write raw CSV results.")
final class BenchCommand implements Callable<Integer> {
    enum Store { local, s3 }

    @Option(names = "--store", defaultValue = "local", description = "local or s3 (uses LEDGER_S3_*).")
    Store store;

    @Option(names = "--out", required = true, description = "Directory for CSV results.")
    Path out;

    @Option(names = "--work", description = "Scratch directory for local tables (default: a temp dir).")
    Path work;

    @Option(names = "--quick", description = "Small sizes, for smoke tests.")
    boolean quick;

    @Override
    public Integer call() throws Exception {
        Bench.StoreFactory factory;
        if (store == Store.local) {
            Path root = work != null ? work : Files.createTempDirectory("ledger-bench");
            factory = name -> new LocalObjectStore(root.resolve(name + "-" + UUID.randomUUID()));
        } else {
            String run = "bench/" + UUID.randomUUID();
            factory = name -> {
                S3Config config = S3Config.fromEnv(run + "/" + name);
                if (config == null) {
                    throw new IllegalArgumentException("--store s3 needs LEDGER_S3_ENDPOINT");
                }
                return S3ObjectStore.connect(config);
            };
        }
        new Bench(factory, store.name(), out, quick).runAll();
        return 0;
    }
}
