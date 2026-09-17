package br.com.datastreambrasil.v4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;

/**
 * JDBC side of v4: column discovery at startup and, when {@code ingestion_only} is false,
 * the MERGE/DELETE statements and the cleanup job.
 *
 * <p>Connects exactly like v2/v3 do: user plus the token in the password property, and everything
 * else - database, schema, role - left to the URL and the user's defaults. So an existing v3
 * connector definition carries over unchanged.
 *
 * <p>The streaming SDK cannot work that way: it needs the database and schema as explicit
 * arguments, and has no password concept. {@link #urlParam} exists so the connector can read them
 * back out of the same JDBC URL instead of asking for them twice.
 */
public class SnowflakeJdbc implements AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(SnowflakeJdbc.class);

    private static final String FIND_COLUMNS = """
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'
        ORDER BY ORDINAL_POSITION
        """;

    private static final String CLIENT_METADATA_USE_SESSION_DATABASE = "CLIENT_METADATA_USE_SESSION_DATABASE";
    private static final String CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX = "CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX";
    private static final String JDBC_QUERY_RESULT_FORMAT = "JDBC_QUERY_RESULT_FORMAT";

    private final Connection connection;
    private final String schemaName;

    public SnowflakeJdbc(String url, String user, String password, String schemaName) {
        this.schemaName = schemaName;
        try {
            var properties = new Properties();
            properties.put("user", user);
            // the PAT goes in the password property, no authenticator - same as v2/v3.
            // db, schema and role are not set here: they come from the URL and the user's
            // default role, exactly as in v3.
            properties.put("password", password);

            // Force the driver to use the session context for metadata, avoiding implicit
            // SHOW COLUMNS / SHOW OBJECTS.
            properties.put(CLIENT_METADATA_USE_SESSION_DATABASE, "true");
            properties.put(CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX, "true");

            // Disable Arrow, use JSON as the result format.
            // Works around ExceptionInInitializerError with sun.misc.Unsafe on Java 17+.
            properties.put(JDBC_QUERY_RESULT_FORMAT, "JSON");

            this.connection = DriverManager.getConnection(url, properties);
        } catch (SQLException e) {
            LOGGER.error("Error while connecting to snowflake connection", e);
            throw new RuntimeException("Error while connecting to snowflake connection", e);
        }
    }

    /** Package-private hook so tests can inject a mocked connection. */
    SnowflakeJdbc(Connection connection, String schemaName) {
        this.connection = connection;
        this.schemaName = schemaName;
    }

    /**
     * Host of a JDBC URL, without the port:
     * {@code jdbc:snowflake://acme.snowflakecomputing.com?db=X} yields
     * {@code acme.snowflakecomputing.com}.
     */
    public static Optional<String> hostFrom(String url) {
        if (url == null) {
            return Optional.empty();
        }
        var schemeEnd = url.indexOf("://");
        if (schemeEnd < 0) {
            return Optional.empty();
        }

        var host = url.substring(schemeEnd + 3);
        var end = host.length();
        for (char delimiter : new char[]{'?', '/'}) {
            var at = host.indexOf(delimiter);
            if (at >= 0 && at < end) {
                end = at;
            }
        }
        host = host.substring(0, end);

        var portAt = host.lastIndexOf(':');
        if (portAt > 0) {
            host = host.substring(0, portAt);
        }

        return host.isBlank() ? Optional.empty() : Optional.of(host);
    }

    /**
     * Account identifier behind a JDBC URL. An explicit {@code account} parameter wins; otherwise
     * it is the first label of the host, which is how the driver itself derives it. That keeps
     * {@code acme.us-east-1...} and {@code acme.privatelink...} resolving to {@code acme}.
     */
    public static Optional<String> accountFrom(String url) {
        var explicit = urlParam(url, "account");
        if (explicit.isPresent()) {
            return explicit;
        }
        return hostFrom(url).map(host -> {
            var dot = host.indexOf('.');
            return dot > 0 ? host.substring(0, dot) : host;
        });
    }

    /**
     * Reads a connection parameter out of a JDBC URL's query string, e.g. the {@code db} of
     * {@code jdbc:snowflake://acme.snowflakecomputing.com?db=DATB200D&schema=LZ_IH}.
     *
     * <p>Several names can be given because the driver accepts synonyms ({@code db} and
     * {@code database}). Matching is case insensitive, like the driver's own.
     */
    public static Optional<String> urlParam(String url, String... names) {
        if (url == null) {
            return Optional.empty();
        }
        var queryStart = url.indexOf('?');
        if (queryStart < 0 || queryStart == url.length() - 1) {
            return Optional.empty();
        }

        var wanted = Arrays.stream(names).map(n -> n.toLowerCase(Locale.ROOT)).toList();
        for (String pair : url.substring(queryStart + 1).split("&")) {
            var separator = pair.indexOf('=');
            if (separator <= 0) {
                continue;
            }
            var key = pair.substring(0, separator).trim().toLowerCase(Locale.ROOT);
            if (!wanted.contains(key)) {
                continue;
            }
            var value = URLDecoder.decode(pair.substring(separator + 1).trim(), StandardCharsets.UTF_8);
            if (!value.isEmpty()) {
                return Optional.of(value);
            }
        }
        return Optional.empty();
    }

    public Connection connection() {
        return connection;
    }

    /**
     * Column names of {@code table}, in ordinal order.
     *
     * @param useJdbcMetadata when true uses {@link java.sql.DatabaseMetaData} (SHOW COLUMNS, which
     *                        needs no running warehouse) instead of querying INFORMATION_SCHEMA.
     */
    public List<String> columnsOf(String table, boolean useJdbcMetadata, List<String> ignoreColumns) {
        try {
            var columns = useJdbcMetadata ? fromJdbcMetadata(table) : fromInformationSchema(table);

            if (columns.isEmpty()) {
                throw new RuntimeException(
                        "Empty columns returned from target table " + table + ", schema " + schemaName);
            }

            columns.removeAll(ignoreColumns);
            var distinct = columns.stream().distinct().toList();

            LOGGER.debug("Columns mapped from table {}: {}", table, String.join(",", distinct));
            return distinct;
        } catch (SQLException e) {
            LOGGER.error("Error while get metadata columns from snowflake", e);
            throw new RuntimeException("Error while get metadata columns from snowflake", e);
        }
    }

    private ArrayList<String> fromInformationSchema(String table) throws SQLException {
        var columns = new ArrayList<String>();
        var sql = String.format(FIND_COLUMNS, schemaName.toUpperCase(), table.toUpperCase());
        try (var stmt = connection.createStatement(); var rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                columns.add(rs.getString("COLUMN_NAME"));
            }
        }
        return columns;
    }

    private ArrayList<String> fromJdbcMetadata(String table) throws SQLException {
        var columns = new ArrayList<String>();
        var metadata = connection.getMetaData();
        try (var rs = metadata.getColumns(null, schemaName.toUpperCase(), table.toUpperCase(), null)) {
            while (rs.next()) {
                columns.add(rs.getString("COLUMN_NAME"));
            }
        }
        return columns;
    }

    @Override
    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            LOGGER.warn("Error while closing snowflake connection", e);
        }
    }
}
