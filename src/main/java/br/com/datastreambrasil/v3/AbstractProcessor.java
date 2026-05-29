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
    protected List<String> excludeIngestAdditionalFields = new ArrayList<>();
    protected List<String> timestampFieldsConvert = new ArrayList<>();
    protected List<String> dateFieldsConvert = new ArrayList<>();
    protected List<String> timeFieldsConvert = new ArrayList<>();
    protected Map<String, CompressedMap<SnowflakeRecord>> buffer = new HashMap<>();
    protected Set<String> knownIngestTables = ConcurrentHashMap.newKeySet();
    protected int bufferInitialCapacity;
    protected String tmpDataFolder;
    protected boolean processMultiTables;
    protected boolean findInColumnsMetadata;
    protected boolean mustProcessReadOnlyMessages;

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
    protected static final String FILE_SEPARATOR = "/";

    protected enum debeziumOperation {
        d,
        c,
        u,
        r
    }

    protected final static String INGEST_SUFFIX = "_INGEST";

    private static final String CLIENT_METADATA_USE_SESSION_DATABASE = "CLIENT_METADATA_USE_SESSION_DATABASE";
    private static final String CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX = "CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX";
    private static final String JDBC_QUERY_RESULT_FORMAT = "JDBC_QUERY_RESULT_FORMAT";

    private static final String FIND_ALL_INGEST_TABLE_COLUMNS = """
        SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME LIKE '%%_INGEST'
        ORDER BY TABLE_NAME, ORDINAL_POSITION
        """;

    protected abstract void extraConfigsOnStart(AbstractConfig config);

    protected abstract void put(Collection<SinkRecord> collection);

    protected abstract void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets);

    protected abstract void stop();

    protected void start(AbstractConfig config) {
        configParameters(config);
        setupSnowflakeConnection(config);

        if (findInColumnsMetadata && !processMultiTables) {
            prepareColumnsFromMetadata(tableName, config);
        } else {
            preloadAllTableColumns();
        }

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
        excludeIngestAdditionalFields.addAll(config.getList(SnowflakeSinkConnector.CFG_EXCLUDE_INGEST_ADDITIONAL_FIELDS));

        tmpDataFolder = config.getString(SnowflakeSinkConnector.CFG_TMP_DATA_FOLDER);
        bufferInitialCapacity = config.getInt(SnowflakeSinkConnector.CFG_BUFFER_INITIAL_CAPACITY);
        processMultiTables = config.getBoolean(SnowflakeSinkConnector.CFG_PROCESS_MULTIPLE_TABLES);
        findInColumnsMetadata = config.getBoolean(SnowflakeSinkConnector.CFG_FIND_COLUMNS_IN_METADATA);
        mustProcessReadOnlyMessages = config.getBoolean(SnowflakeSinkConnector.CFG_MUST_PROCESS_READ_ONLY_MESSAGES);
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

            if (!findInColumnsMetadata) {
                // Desabilita Arrow, usa JSON como formato de resultado
                // Resolve ExceptionInInitializerError com sun.misc.Unsafe no Java 17+
                // Ref: https://docs.snowflake.com/en/developer-guide/jdbc/jdbc-configure
                properties.put(JDBC_QUERY_RESULT_FORMAT, "JSON");
                LOGGER.warn("Find columns by information_schema query to schema: {}", schemaName);
            }

            connection = DriverManager.getConnection(config.getString(SnowflakeSinkConnector.CFG_URL), properties);
            snowflakeConnection = connection.unwrap(SnowflakeConnection.class);
        } catch (SQLException e) {
            LOGGER.error("Error while connecting to snowflake connection", e);
            throw new RuntimeException("Error while connecting to snowflake connection", e);
        }
    }

    protected String extractTableName(String topic) {
        if (topic != null && topic.contains(".") && processMultiTables) {
            return topic.substring(topic.lastIndexOf(".") + 1);
        }
        return tableName;
    }

    void preloadAllTableColumns() {
        String sql = String.format(FIND_ALL_INGEST_TABLE_COLUMNS, schemaName.toUpperCase());
        Map<String, List<String>> rawByIngestTable = new HashMap<>();

        try (var stmt = connection.createStatement(); var rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                rawByIngestTable
                        .computeIfAbsent(rs.getString("TABLE_NAME"), k -> new ArrayList<>())
                        .add(rs.getString("COLUMN_NAME"));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error pre-loading ingest table columns for schema " + schemaName, e);
        }

        if (rawByIngestTable.isEmpty()) {
            throw new RuntimeException("No _INGEST tables found in schema: " + schemaName + " during pre-load");
        }

        for (var entry : rawByIngestTable.entrySet()) {
            String ingestTableName = entry.getKey();
            String baseTableName = ingestTableName.substring(0, ingestTableName.length() - INGEST_SUFFIX.length());

            List<String> ingestCols = new ArrayList<>(entry.getValue());
            ingestCols.removeAll(ignoreColumns);

            List<String> finalCols = ingestCols.stream()
                    .filter(col -> excludeIngestAdditionalFields.stream()
                            .noneMatch(excluded -> excluded.equalsIgnoreCase(col)))
                    .toList();

            columnsIngestTable.put(ingestTableName, ingestCols);
            columnsFinalTable.put(baseTableName, finalCols);
            knownIngestTables.add(ingestTableName);

            LOGGER.debug("Pre-loaded {} columns for ingest table: {} - Columns names: {}",
                    ingestCols.size(), ingestTableName, ingestCols);
            LOGGER.debug("Pre-loaded {} columns from ingest table: {} for final table: {} - Columns names: {}",
                    finalCols.size(), ingestTableName, baseTableName, finalCols);
        }

        LOGGER.info("Pre-loaded columns for {} ingest tables in schema {}", rawByIngestTable.size(), schemaName);
    }

    protected CompressedMap<SnowflakeRecord> getOrCreateBuffer(String tableBaseName) {
        return buffer.computeIfAbsent(tableBaseName, k -> {
            knownIngestTables.add(String.format("%s%s", tableBaseName, INGEST_SUFFIX));
            return new CompressedMap<>(new KryoFactory(), bufferInitialCapacity);
        });
    }

    protected void prepareColumnsFromMetadata(String table, AbstractConfig config) {
        try {
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

            LOGGER.debug("Columns: {} mapped from target table: {}", String.join(LINE_SEPARATOR_COMMA, columnsNoDuplicate), table);

            var ingestTableName = String.format("%s%s", table, INGEST_SUFFIX);
            var columnsFromIngestTable = new ArrayList<String>();
            columnsFromIngestTable.addAll(columnsFromTable);
            columnsFromIngestTable.addAll(config.getList(SnowflakeSinkConnector.CFG_EXCLUDE_INGEST_ADDITIONAL_FIELDS));

            columnsIngestTable.put(ingestTableName, columnsFromIngestTable);
            knownIngestTables.add(ingestTableName);
            columnsFinalTable.put(table, columnsFromTable);
        } catch (SQLException e) {
            throw new RuntimeException("Error pre-loading ingest table columns for schema " + schemaName, e);
        }
    }
}
