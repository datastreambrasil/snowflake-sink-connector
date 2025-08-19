package br.com.datastreambrasil;

import net.snowflake.client.jdbc.SnowflakeConnection;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;

public class SnowflakeSinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeSinkConnector.class);
    private Connection connection;
    private SnowflakeConnection snowflakeConnection;
    private String stageName;
    private String tableName;
    private String schemaName;
    private final Collection<Map<String, Object>> buffer = new ArrayList<>();
    private static final String PAYLOAD = "payload";
    private static final String AFTER = "after";
    private static final String BEFORE = "before";
    private static final String OP = "op";
    private static final String IHTOPIC = "ih_topic";
    private static final String IHOFFSET = "ih_offset";
    private static final String IHPARTITION = "ih_partition";
    private static final String IHOP = "ih_op";
    private static final String DELETE = "d";
    private List<String> columnsFromTable;

    @Override
    public String version() {
        return SnowflakeSinkConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> map) {
        try {
            //init configs
            stageName = map.get(SnowflakeSinkConnector.CFG_STAGE_NAME);
            tableName = map.get(SnowflakeSinkConnector.CFG_TABLE_NAME);
            schemaName = map.get(SnowflakeSinkConnector.CFG_SCHEMA_NAME);

            //init connection
            var properties = new Properties();
            properties.put("user", map.get(SnowflakeSinkConnector.CFG_USER));
            properties.put("password", map.get(SnowflakeSinkConnector.CFG_PASSWORD));
            connection = DriverManager.getConnection(map.get(SnowflakeSinkConnector.CFG_URL), properties);
            snowflakeConnection = connection.unwrap(SnowflakeConnection.class);
        } catch (Throwable e) {
            LOGGER.error("Error while starting Snowflake connector", e);
            throw new RuntimeException("Error while starting Snowflake connector", e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void put(Collection<SinkRecord> collection) {
        try {
            for (SinkRecord record : collection) {
                var mapValue = (Map<String, Object>) record.value();

                validateFieldOnMap(PAYLOAD, mapValue);
                var mapPayload = (Map<String, Object>) mapValue.get(PAYLOAD);

                validateFieldOnMap(OP, mapPayload);
                var op = mapPayload.get(OP);
                Map<String, Object> mapPayloadAfterBefore;
                if (op.equals(DELETE)) {
                    validateFieldOnMap(BEFORE, mapPayload);
                    mapPayloadAfterBefore = (Map<String, Object>) mapPayload.get(BEFORE);
                } else {
                    validateFieldOnMap(AFTER, mapPayload);
                    mapPayloadAfterBefore = (Map<String, Object>) mapPayload.get(AFTER);
                }

                if (record.topic() == null || record.kafkaPartition() == null) {
                    LOGGER.error("Null values for topic or kafkaPartition. Topic {}, KafkaPartition {}", record.topic(), record.kafkaPartition());
                    throw new RuntimeException("Null values for topic or kafkaPartition");
                }

                //topic,partition,offset,operation
                mapPayloadAfterBefore.put(IHTOPIC, record.topic());
                mapPayloadAfterBefore.put(IHPARTITION, record.kafkaPartition());
                mapPayloadAfterBefore.put(IHOFFSET, String.valueOf(record.kafkaOffset()));
                mapPayloadAfterBefore.put(IHOP, String.valueOf(mapPayload.get(OP)));


                buffer.add(mapPayloadAfterBefore);
            }
        } catch (Throwable e) {
            LOGGER.error("Error while putting records", e);
            throw new RuntimeException("Error while putting records", e);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {

        if (buffer.isEmpty()) {
            return;
        }

        var destFileName = UUID.randomUUID().toString();
        try {
            LOGGER.debug("Preparing to send {} records from buffer. To stage {} and table {}", buffer.size(), stageName, tableName);


            try (var csvToInsert = prepareOrderedColumnsBasedOnTargetTable();
                 var inputStream = new ByteArrayInputStream(csvToInsert.toByteArray())) {
                snowflakeConnection.uploadStream(stageName, "/", inputStream,
                        destFileName, true);
                try (var stmt = connection.createStatement()) {
                    String copyInto = String.format("COPY INTO %s FROM @%s/%s.gz PURGE = TRUE", tableName, stageName, destFileName);
                    LOGGER.debug("Copying statement: {}", copyInto);
                    stmt.executeUpdate(copyInto);
                }
            }


        } catch (Throwable e) {
            try {
                try (var stmt = connection.createStatement()) {
                    String removeFileFromStage = String.format("REMOVE @%s/%s.gz", stageName, destFileName);
                    stmt.execute(removeFileFromStage);
                }
            } catch (Throwable e2) {
                throw new RuntimeException("Error while removing file [" + destFileName + "] from stage " + stageName, e2);
            }

            LOGGER.error("Error while flushing Snowflake connector", e);
            throw new RuntimeException("Error while flushing", e);
        } finally {
            buffer.clear();
        }
    }


    @Override
    public void stop() {
    }

    private void validateFieldOnMap(String fieldToValidate, Map<String, ?> map) {
        if (!map.containsKey(fieldToValidate) || map.get(fieldToValidate) == null) {
            LOGGER.error("Key [{}] is missing or null on json", fieldToValidate);
            throw new RuntimeException("missing or null key [" + fieldToValidate + "] on json");
        }
    }

    private ByteArrayOutputStream prepareOrderedColumnsBasedOnTargetTable() throws Throwable {
        var metadata = connection.getMetaData();

        if (columnsFromTable == null) {
            columnsFromTable = new ArrayList<>();
            try (var rsColumns = metadata.getColumns(null, schemaName.toUpperCase(), tableName.toUpperCase(), null)) {
                while (rsColumns.next()) {
                    columnsFromTable.add(rsColumns.getString("COLUMN_NAME").toUpperCase());
                }
            }
        }

        if (columnsFromTable.isEmpty()) {
            throw new RuntimeException("Empty columns returned from target table " + tableName + ", schema " + schemaName);
        }

        LOGGER.debug("Columns mapped from target table: {}", String.join(",", columnsFromTable));

        var csvInMemory = new ByteArrayOutputStream();

        for (var recordInBuffer : buffer) {
            for (int i = 0; i < columnsFromTable.size(); i++) {
                if (recordInBuffer.containsKey(columnsFromTable.get(i).toUpperCase())) {
                    var valueFromRecord = recordInBuffer.get(columnsFromTable.get(i).toUpperCase());
                    if (valueFromRecord != null) {
                        var strBuffer = "\"" + valueFromRecord + "\"";
                        csvInMemory.writeBytes(strBuffer.getBytes());
                    }
                } else if (recordInBuffer.containsKey(columnsFromTable.get(i).toLowerCase())) {
                    var valueFromRecord = recordInBuffer.get(columnsFromTable.get(i).toLowerCase());
                    if (valueFromRecord != null) {
                        var strBuffer = "\"" + valueFromRecord + "\"";
                        csvInMemory.writeBytes(strBuffer.getBytes());
                    }
                } else {
                    LOGGER.warn("Column {} not found on buffer, inserted empty value", columnsFromTable.get(i));
                }

                if (i < columnsFromTable.size() - 1) {
                    csvInMemory.writeBytes(",".getBytes());
                }
            }

            csvInMemory.writeBytes("\n".getBytes());
        }

        return csvInMemory;
    }
}
