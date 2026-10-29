package dev.ledgertable.store;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Object store over a local directory. {@link #putIfAbsent} writes the payload to a private temp
 * file, fsyncs it, and then publishes it with a hard link. {@code link(2)} fails if the target
 * exists and is atomic, so concurrent writers in any number of processes see exactly one winner
 * and nobody ever observes a partial object.
 */
public final class LocalObjectStore implements ObjectStore {
    private static final String TMP_DIR = ".tmp";

    private final Path root;
    private final Path tmp;

    public LocalObjectStore(Path root) throws IOException {
        this.root = root.toAbsolutePath().normalize();
        this.tmp = this.root.resolve(TMP_DIR);
        Files.createDirectories(tmp);
    }

    @Override
    public boolean putIfAbsent(String key, byte[] data) throws IOException {
        Path target = resolve(key);
        Files.createDirectories(target.getParent());
        Path staged = stage(data);
        try {
            Files.createLink(target, staged);
            return true;
        } catch (FileAlreadyExistsException e) {
            return false;
        } finally {
            Files.deleteIfExists(staged);
        }
    }

    @Override
    public void put(String key, byte[] data) throws IOException {
        Path target = resolve(key);
        Files.createDirectories(target.getParent());
        Path staged = stage(data);
        try {
            Files.move(staged, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } finally {
            Files.deleteIfExists(staged);
        }
    }

    @Override
    public Optional<byte[]> get(String key) throws IOException {
        try {
            return Optional.of(Files.readAllBytes(resolve(key)));
        } catch (NoSuchFileException e) {
            return Optional.empty();
        }
    }

    @Override
    public List<String> list(String prefix, String startAfter) throws IOException {
        int slash = prefix.lastIndexOf('/');
        Path dir = slash < 0 ? root : resolve(prefix.substring(0, slash));
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        var keys = new ArrayList<String>();
        try (Stream<Path> files = Files.walk(dir)) {
            files.filter(Files::isRegularFile).forEach(p -> {
                String key = root.relativize(p).toString().replace('\\', '/');
                if (!key.startsWith(TMP_DIR + "/") && key.startsWith(prefix) && key.compareTo(startAfter) > 0) {
                    keys.add(key);
                }
            });
        } catch (UncheckedIOException e) {
            // A concurrent delete raced with the walk; retry once the directory is stable.
            return list(prefix, startAfter);
        }
        Collections.sort(keys);
        return keys;
    }

    @Override
    public void delete(String key) throws IOException {
        Files.deleteIfExists(resolve(key));
    }

    @Override
    public Optional<Path> localPath(String key) {
        return Optional.of(resolve(key));
    }

    private Path stage(byte[] data) throws IOException {
        Path staged = tmp.resolve(UUID.randomUUID().toString());
        try (FileChannel ch = FileChannel.open(staged, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer buf = ByteBuffer.wrap(data);
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
            ch.force(true);
        }
        return staged;
    }

    private Path resolve(String key) {
        Path p = root.resolve(key).normalize();
        if (!p.startsWith(root) || key.startsWith(TMP_DIR + "/")) {
            throw new IllegalArgumentException("invalid key: " + key);
        }
        return p;
    }
}
