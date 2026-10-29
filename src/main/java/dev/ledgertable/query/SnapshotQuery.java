package dev.ledgertable.query;

import dev.ledgertable.core.LedgerTable;
import dev.ledgertable.core.Snapshot;
import dev.ledgertable.log.AddFile;
import dev.ledgertable.store.ObjectStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Runs DuckDB SQL over one snapshot. The snapshot's live files are exposed as the view
 * {@code usage(event_id, account_id, resource_id, compute_millis, event_time)}; DuckDB reads
 * files in place on a local store and from a private download cache otherwise.
 */
public final class SnapshotQuery implements AutoCloseable {
    public static final String VIEW = "usage";

    private final Connection connection;
    private final ObjectStore store;
    private final Path cache;

    private SnapshotQuery(ObjectStore store) throws SQLException, IOException {
        this.connection = DriverManager.getConnection("jdbc:duckdb:");
        this.store = store;
        this.cache = Files.createTempDirectory("ledger-query");
    }

    public static SnapshotQuery open(LedgerTable table, Snapshot snapshot) throws IOException, SQLException {
        var q = new SnapshotQuery(table.store());
        try {
            q.createView(VIEW, snapshot.files());
        } catch (IOException | SQLException | RuntimeException e) {
            q.close();
            throw e;
        }
        return q;
    }

    /** An engine with no views yet, for queries that define their own over file sets. */
    public static SnapshotQuery empty(LedgerTable table) throws IOException, SQLException {
        return new SnapshotQuery(table.store());
    }

    /** Defines {@code name} as the union of {@code files}; an empty set yields an empty view with the usage schema. */
    public void createView(String name, Collection<AddFile> files) throws IOException, SQLException {
        String source;
        if (files.isEmpty()) {
            source = "SELECT NULL::VARCHAR AS event_id, NULL::VARCHAR AS account_id, NULL::VARCHAR AS resource_id, "
                    + "NULL::BIGINT AS compute_millis, NULL::TIMESTAMPTZ AS event_time WHERE false";
        } else {
            var paths = new ArrayList<String>(files.size());
            for (AddFile f : files) {
                paths.add("'" + localFile(f.path()).toString().replace("'", "''") + "'");
            }
            source = "SELECT event_id, account_id, resource_id, compute_millis, event_time FROM read_parquet(["
                    + String.join(", ", paths) + "])";
        }
        try (Statement st = connection.createStatement()) {
            st.execute("CREATE OR REPLACE VIEW " + name + " AS " + source);
        }
    }

    /** Runs {@code sql} and returns every row as column name to value, in column order. */
    public List<Map<String, Object>> query(String sql) throws SQLException {
        var rows = new ArrayList<Map<String, Object>>();
        try (Statement st = connection.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            while (rs.next()) {
                var row = new LinkedHashMap<String, Object>();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    row.put(md.getColumnLabel(i), rs.getObject(i));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    public Connection connection() {
        return connection;
    }

    private Path localFile(String key) throws IOException {
        var local = store.localPath(key);
        if (local.isPresent()) {
            return local.get();
        }
        Path target = cache.resolve(key);
        if (!Files.exists(target)) {
            Files.createDirectories(target.getParent());
            Files.write(target, store.get(key).orElseThrow(() -> new IOException("live data file is missing: " + key)));
        }
        return target;
    }

    @Override
    public void close() throws SQLException, IOException {
        try {
            connection.close();
        } finally {
            try (Stream<Path> files = Files.walk(cache)) {
                for (Path p : files.sorted(Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(p);
                }
            }
        }
    }
}
