package br.com.datastreambrasil.v3;

import br.com.datastreambrasil.v3.compress.CompressedMap;
import br.com.datastreambrasil.v3.compress.KryoFactory;
import br.com.datastreambrasil.v3.model.SnowflakeRecord;
import net.snowflake.client.jdbc.SnowflakeConnection;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractProcessor {

    protected final static Logger LOGGER = LogManager.getLogger(AbstractProcessor.class);

    protected Connection connection;
    protected SnowflakeConnection snowflakeConnection;
    protected String stageName;
    protected String tableName;
    protected String schemaName;
    protected Map<String, List<String>> columnsFinalTable = new HashMap<>();
    protected Map<String, List<String>> columnsIngestTable = new HashMap<>();
    protected List<String> ignoreColumns = new ArrayList<>();
    protected List<String> timestampFieldsConvert = new ArrayList<>();
    protected List<String> dateFieldsConvert = new ArrayList<>();
    protected List<String> timeFieldsConvert = new ArrayList<>();
    protected Map<String, CompressedMap<SnowflakeRecord>> buffer = new HashMap<>();
    protected Set<String> knownIngestTables = ConcurrentHashMap.newKeySet();
    protected int bufferInitialCapacity;
    protected String tmpDataFolder;

    protected static final String AFTER = "after";
    protected static final String BEFORE = "before";
    protected static final String OP = "op";
    protected static final String IHTOPIC = "ih_topic";
    protected static final String IHOFFSET = "ih_offset";
    protected static final String IHPARTITION = "ih_partition";
    protected static final String IHOP = "ih_op";
    protected static final String IHDATETIME = "ih_datetime";
    protected static final String IHBLOCKID = "ih_blockid";

    protected static final String DOUBLE_QUOTE = "\"";
    protected static final String BREAK_LINE = "\n";
    protected static final String REGEX_REPLACEMENT_QUOTE_VALUE = "\"\"";
    protected static final String LINE_SEPARATOR_COMMA = ",";
    protected static final int SB_CSV_INITIAL_SIZE = 1000;

    protected enum debeziumOperation {
        d,
        c,
        u,
        r
    }

    protected final static String INGEST_SUFFIX = "_INGEST";

    private static final String CLIENT_METADATA_USE_SESSION_DATABASE = "CLIENT_METADATA_USE_SESSION_DATABASE";
    private static final String CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX = "CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX";

    protected abstract void extraConfigsOnStart(AbstractConfig config);

    protected abstract void put(Collection<SinkRecord> collection);

    protected abstract void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets);

    protected abstract void stop();

    protected void start(AbstractConfig config) {
        configParameters(config);
        setupSnowflakeConnection(config);
        extraConfigsOnStart(config);
    }

    protected void configParameters(AbstractConfig config) {
        stageName = config.getString(SnowflakeSinkConnector.CFG_STAGE_NAME);
        tableName = config.getString(SnowflakeSinkConnector.CFG_TABLE_NAME);
        schemaName = config.getString(SnowflakeSinkConnector.CFG_SCHEMA_NAME);

        timestampFieldsConvert.addAll(config.getList(SnowflakeSinkConnector.CFG_TIMESTAMP_FIELDS_CONVERT));
        dateFieldsConvert.addAll(config.getList(SnowflakeSinkConnector.CFG_DATE_FIELDS_CONVERT));
        timeFieldsConvert.addAll(config.getList(SnowflakeSinkConnector.CFG_TIME_FIELDS_CONVERT));
        ignoreColumns.addAll(config.getList(SnowflakeSinkConnector.CFG_IGNORE_COLUMNS));

        tmpDataFolder = config.getString(SnowflakeSinkConnector.CFG_TMP_DATA_FOLDER);
        bufferInitialCapacity = config.getInt(SnowflakeSinkConnector.CFG_BUFFER_INITIAL_CAPACITY);
    }

    protected void setupSnowflakeConnection(AbstractConfig config) {
        try {
            var properties = new Properties();
            properties.put("user", config.getString(SnowflakeSinkConnector.CFG_USER));
            properties.put("password", config.getString(SnowflakeSinkConnector.CFG_PASSWORD));

            // Forca o driver a usar o contexto da sessao para metadata
            // evitando SHOW COLUMNS / SHOW OBJECTS implicitos
            properties.put(CLIENT_METADATA_USE_SESSION_DATABASE, "true");
            properties.put(CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX, "true");

            connection = DriverManager.getConnection(config.getString(SnowflakeSinkConnector.CFG_URL), properties);
            snowflakeConnection = connection.unwrap(SnowflakeConnection.class);
        } catch (SQLException e) {
            LOGGER.error("Error while connecting to snowflake connection", e);
            throw new RuntimeException("Error while connecting to snowflake connection", e);
        }
    }

    /**
     * Returns the table name extracted from the topic (value after the last dot).
     * Falls back to the configured tableName when the topic has no dot separator.
     */
    protected String extractTableNameFromTopic(String topic) {
        if (topic != null && topic.contains(".")) {
            return topic.substring(topic.lastIndexOf(".") + 1);
        }
        return tableName;
    }

    /**
     * Lazily loads and caches column metadata for a table using the JDBC metadata API.
     * No-op if columns for the table are already cached.
     */
    protected void ensureColumnsForTable(String tableBaseName) {
        if (columnsIngestTable.containsKey(tableBaseName)) {
            return;
        }
        try {
            String ingestTable = tableBaseName + INGEST_SUFFIX;
            columnsIngestTable.put(tableBaseName, getColumnsFromMetadata(ingestTable));
            columnsFinalTable.put(tableBaseName, getColumnsFromMetadata(tableBaseName));
        } catch (SQLException e) {
            LOGGER.error("Error while loading metadata columns for table {}", tableBaseName, e);
            throw new RuntimeException("Error while loading metadata columns for table " + tableBaseName, e);
        }
    }

    protected CompressedMap<SnowflakeRecord> getOrCreateBuffer(String tableBaseName) {
        return buffer.computeIfAbsent(tableBaseName, k -> {
            knownIngestTables.add(tableBaseName + INGEST_SUFFIX);
            return new CompressedMap<>(new KryoFactory(), bufferInitialCapacity);
        });
    }

    protected List<String> getColumnsFromMetadata(String table) throws SQLException {
        var metadata = connection.getMetaData();

        var columnsFromTable = new ArrayList<String>();
        try (var rsColumns = metadata.getColumns(null, schemaName.toUpperCase(), table.toUpperCase(), null)) {
            while (rsColumns.next()) {
                columnsFromTable.add(rsColumns.getString("COLUMN_NAME"));
            }
        }
        if (columnsFromTable.isEmpty()) {
            throw new RuntimeException(
                "Empty columns returned from target table " + table + ", schema " + schemaName);
        }

        columnsFromTable.removeAll(ignoreColumns);
        var columnsNoDuplicate = columnsFromTable.stream().distinct().toList();

        LOGGER.debug("Columns mapped from target table: {}", String.join(LINE_SEPARATOR_COMMA, columnsNoDuplicate));

        return columnsNoDuplicate;
    }
}
