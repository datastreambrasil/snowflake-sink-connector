package br.com.datastreambrasil.v3;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public abstract class AbstractProcessor {

    protected final static Logger LOGGER = LogManager.getLogger(AbstractProcessor.class);

    protected Connection connection;
    protected SnowflakeConnection snowflakeConnection;
    protected String stageName;
    protected String tableName;
    protected String ingestTableName;
    protected String schemaName;
    protected List<String> columnsFinalTable = new ArrayList<>();
    protected List<String> columnsIngestTable = new ArrayList<>();
    protected List<String> ignoreColumns = new ArrayList<>();
    protected List<String> timestampFieldsConvert = new ArrayList<>();
    protected List<String> dateFieldsConvert = new ArrayList<>();
    protected List<String> timeFieldsConvert = new ArrayList<>();
    protected final Map<String, SnowflakeRecord> buffer = new LinkedHashMap<>();
    protected boolean hashingSupport;


    protected static final String AFTER = "after";
    protected static final String BEFORE = "before";
    protected static final String OP = "op";
    protected static final String IHTOPIC = "ih_topic";
    protected static final String IHOFFSET = "ih_offset";
    protected static final String IHPARTITION = "ih_partition";
    protected static final String IHOP = "ih_op";
    protected static final String IHDATETIME = "ih_datetime";
    protected static final String IHBLOCKID = "ih_blockid";
    protected static final String IH_PREVIOUS_HASH = "ih_previous_hash";
    protected static final String IH_CURRENT_HASH = "ih_current_hash";

    protected enum debeziumOperation {
        d,
        c,
        u,
        r
    }

    protected final static String INGEST_SUFFIX = "_INGEST";

    protected abstract void extraConfigsOnStart(AbstractConfig config);

    protected abstract void put(Collection<SinkRecord> collection);

    protected abstract void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets);

    protected abstract void stop();

    protected void start(AbstractConfig config) {
        configParameters(config);
        setupSnowflakeConnection(config);
        configMetadata();
        extraConfigsOnStart(config);
    }

    protected void configMetadata() {
        try {
            columnsFinalTable = getColumnsFromMetadata(tableName);
            columnsIngestTable = getColumnsFromMetadata(ingestTableName);
        } catch (SQLException e) {
            LOGGER.error("Error while get metadata columns from snowflake", e);
            throw new RuntimeException("Error while get metadata columns from snowflake", e);
        }
    }

    protected void configParameters(AbstractConfig config) {
        stageName = config.getString(SnowflakeSinkConnector.CFG_STAGE_NAME);
        tableName = config.getString(SnowflakeSinkConnector.CFG_TABLE_NAME);
        ingestTableName = tableName + INGEST_SUFFIX;
        schemaName = config.getString(SnowflakeSinkConnector.CFG_SCHEMA_NAME);
        hashingSupport = config.getBoolean(SnowflakeSinkConnector.CFG_HASHING_SUPPORT);

        timestampFieldsConvert.addAll(config.getList(SnowflakeSinkConnector.CFG_TIMESTAMP_FIELDS_CONVERT));
        dateFieldsConvert.addAll(config.getList(SnowflakeSinkConnector.CFG_DATE_FIELDS_CONVERT));
        timeFieldsConvert.addAll(config.getList(SnowflakeSinkConnector.CFG_TIME_FIELDS_CONVERT));
        ignoreColumns.addAll(config.getList(SnowflakeSinkConnector.CFG_IGNORE_COLUMNS));

    }

    protected void setupSnowflakeConnection(AbstractConfig config) {
        try {
            var properties = new Properties();
            properties.put("user", config.getString(SnowflakeSinkConnector.CFG_USER));
            properties.put("password", config.getString(SnowflakeSinkConnector.CFG_PASSWORD));
            connection = DriverManager.getConnection(config.getString(SnowflakeSinkConnector.CFG_URL), properties);
            snowflakeConnection = connection.unwrap(SnowflakeConnection.class);   // using the provided configuration.
        } catch (SQLException e) {
            LOGGER.error("Error while connecting to snowflake connection", e);
            throw new RuntimeException("Error while connecting to snowflake connection", e);
        }
    }

    private List<String> getColumnsFromMetadata(String table) throws SQLException {
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
        //remove duplicated
        var columnsNoDuplicate = columnsFromTable.stream().distinct().toList();

        LOGGER.debug("Columns mapped from target table: {}", String.join(",", columnsNoDuplicate));


        return columnsNoDuplicate;
    }
}
