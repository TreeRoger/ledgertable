package dev.ledgertable.cli;

import dev.ledgertable.store.LocalObjectStore;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.util.concurrent.Callable;

@Command(name = "export", description = "Copy every object of a table into a local directory (used to check S3 runs).")
final class ExportCommand implements Callable<Integer> {
    @Mixin
    StoreOptions store;

    @Option(names = "--to", required = true)
    Path target;

    @Override
    public Integer call() throws Exception {
        try (var source = store.open(); var sink = new LocalObjectStore(target)) {
            for (String key : source.list("")) {
                sink.putIfAbsent(key, source.get(key).orElseThrow());
            }
        }
        return 0;
    }
}
