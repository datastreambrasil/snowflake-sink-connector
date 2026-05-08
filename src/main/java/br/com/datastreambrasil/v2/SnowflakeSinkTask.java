package br.com.datastreambrasil.v2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.UUID;
import net.snowflake.client.jdbc.SnowflakeConnection;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
public class SnowflakeSinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeSinkConnector.class);
    private Connection connection;
    private SnowflakeConnection snowflakeConnection;
    private String stageName;
    private String tableName;
    private String schemaName;
    private boolean cdcFormat;
    private boolean truncateBeforeBulk;
    private int truncateWhenNoDataAfterSeconds;
    private LocalDateTime lastFlush = LocalDateTime.now();

    private final List<String> timestampFieldsConvertToSeconds = new ArrayList<>();
    private final List<String> dateFieldsConvert = new ArrayList<>();
    private final List<String> timeFieldsConvert = new ArrayList<>();

    private final List<String> pks = new ArrayList<>();
    private final List<String> ignoreColumns = new ArrayList<>();
    private final Collection<Map<String, Object>> buffer = new ArrayList<>();
    private static final String AFTER = "after";
    private static final String BEFORE = "before";
    private static final String OP = "op";
    private static final String IHTOPIC = "ih_topic";
    private static final String IHOFFSET = "ih_offset";
    private static final String IHPARTITION = "ih_partition";
    private static final String IHOP = "ih_op";
    private static final String DELETE = "d";
    private Scheduler scheduler;
    private boolean removeStageAfterCopy;

    // quartz constants
    protected static final String KEY_SNOWFLAKE_CONNECTION = "snowflakeConnection";

    private List<String> columnsFinalTable = new ArrayList<>();

    @Override
    public String version() {
        return SnowflakeSinkConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> map) {
        try {
            AbstractConfig config = new AbstractConfig(SnowflakeSinkConnector.CONFIG_DEF, map);

            // init configs
            stageName = config.getString(SnowflakeSinkConnector.CFG_STAGE_NAME);
            tableName = config.getString(SnowflakeSinkConnector.CFG_TABLE_NAME);
            schemaName = config.getString(SnowflakeSinkConnector.CFG_SCHEMA_NAME);
            cdcFormat = config.getBoolean(SnowflakeSinkConnector.CFG_PAYLOAD_CDC_FORMAT);
            truncateBeforeBulk = config.getBoolean(SnowflakeSinkConnector.CFG_ALWAYS_TRUNCATE_BEFORE_BULK);
            truncateWhenNoDataAfterSeconds = config
                    .getInt(SnowflakeSinkConnector.CFG_TRUNCATE_WHEN_NODATA_AFTER_SECONDS);
            removeStageAfterCopy = config.getBoolean(SnowflakeSinkConnector.CFG_REMOVE_STAGE_AFTER_COPY);

            var disableCleanUpJob = config.getBoolean(SnowflakeSinkConnector.CFG_JOB_CLEANUP_DISABLE);
            var intervalHoursCleanup = config.getInt(SnowflakeSinkConnector.CFG_JOB_CLEANUP_HOURS);

            if (map.containsKey(SnowflakeSinkConnector.CFG_TIMESTAMP_FIELDS_CONVERT_SECONDS)) {
                timestampFieldsConvertToSeconds.addAll(
                        Arrays.stream(map.get(SnowflakeSinkConnector.CFG_TIMESTAMP_FIELDS_CONVERT_SECONDS).split(","))
                                .toList());
            }

            if (map.containsKey(SnowflakeSinkConnector.CFG_DATE_FIELDS_CONVERT)) {
                dateFieldsConvert.addAll(
                        Arrays.stream(map.get(SnowflakeSinkConnector.CFG_DATE_FIELDS_CONVERT).split(","))
                                .toList());
            }

            if (map.containsKey(SnowflakeSinkConnector.CFG_TIME_FIELDS_CONVERT)) {
                timeFieldsConvert.addAll(
                        Arrays.stream(map.get(SnowflakeSinkConnector.CFG_TIME_FIELDS_CONVERT).split(","))
                                .toList());
            }

            if (map.containsKey(SnowflakeSinkConnector.CFG_PK)) {
                pks.addAll(
                        Arrays.stream(map.get(SnowflakeSinkConnector.CFG_PK).split(","))
                                .toList());
            }

            if (map.containsKey(SnowflakeSinkConnector.CFG_IGNORE_COLUMNS)) {
                ignoreColumns.addAll(
                        Arrays.stream(map.get(SnowflakeSinkConnector.CFG_IGNORE_COLUMNS).split(","))
                                .toList());
            }

            // init connection
            var properties = new Properties();
            properties.put("user", map.get(SnowflakeSinkConnector.CFG_USER));
            properties.put("password", map.get(SnowflakeSinkConnector.CFG_PASSWORD));
            connection = DriverManager.getConnection(map.get(SnowflakeSinkConnector.CFG_URL), properties);
            snowflakeConnection = connection.unwrap(SnowflakeConnection.class);

            //fill columns
            columnsFinalTable = getColumnsFromMetadata(tableName);

            // job quartz config
            if (!disableCleanUpJob) {
                var jobData = new HashMap<String, Object>();
                jobData.put(KEY_SNOWFLAKE_CONNECTION, connection);
                jobData.put(SnowflakeSinkConnector.CFG_TABLE_NAME, tableName);
                jobData.put(SnowflakeSinkConnector.CFG_PK, pks);

                var uuid = UUID.randomUUID().toString();
                var props = new Properties();
                props.setProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME, "cleanup_" + uuid);
                props.setProperty("org.quartz.threadPool.threadCount", "1");

                var schedulerFactory = new StdSchedulerFactory(props);
                scheduler = schedulerFactory.getScheduler();
                var job = JobBuilder.newJob(CleanupJob.class).withIdentity("cleanupjob")
                        .setJobData(new JobDataMap(jobData))
                        .build();
                var trigger = TriggerBuilder.newTrigger().withIdentity("trigger_cleanupjob")
                        .withSchedule(SimpleScheduleBuilder.repeatHourlyForever(intervalHoursCleanup))
                        .build();
                scheduler.scheduleJob(job, trigger);
                scheduler.start();
            } else {
                LOGGER.warn("Cleanup job is disabled");
            }

        } catch (Throwable e) {
            LOGGER.error("Error while starting Snowflake connector", e);
            throw new RuntimeException("Error while starting Snowflake connector", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        try {
            for (SinkRecord record : collection) {

                if (record.topic() == null || record.kafkaPartition() == null) {
                    LOGGER.error("Null values for topic or kafkaPartition. Topic {}, KafkaPartition {}", record.topic(),
                            record.kafkaPartition());
                    throw new RuntimeException("Null values for topic or kafkaPartition");
                }

                var mapValue = (Map<String, Object>) record.value();

                if (cdcFormat) {
                    addRecordUsingCDCFormat(mapValue, record);
                } else {
                    addRecordUsingPlainFormat(mapValue, record);
                }
            }
        } catch (Throwable e) {
            LOGGER.error("Error while putting records", e);
            throw new RuntimeException("Error while putting records", e);
        }
    }

    private void addRecordUsingPlainFormat(Map<String, Object> mapValue, SinkRecord record) {
        var mapCaseInsensitive = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        mapCaseInsensitive.putAll(mapValue);

        // topic,partition,offset,operation
        mapCaseInsensitive.put(IHTOPIC, record.topic());
        mapCaseInsensitive.put(IHPARTITION, record.kafkaPartition());
        mapCaseInsensitive.put(IHOFFSET, String.valueOf(record.kafkaOffset()));
        mapCaseInsensitive.put(IHOP, "c");

        buffer.add(mapCaseInsensitive);
    }

    private void addRecordUsingCDCFormat(Map<String, Object> mapValue, SinkRecord record) {
        validateFieldOnMap(OP, mapValue);
        var op = mapValue.get(OP);
        Map<String, Object> mapPayloadAfterBefore;
        if (op.equals(DELETE)) {
            validateFieldOnMap(BEFORE, mapValue);
            mapPayloadAfterBefore = (Map<String, Object>) mapValue.get(BEFORE);
        } else {
            validateFieldOnMap(AFTER, mapValue);
            mapPayloadAfterBefore = (Map<String, Object>) mapValue.get(AFTER);
        }

        // topic,partition,offset,operation
        mapPayloadAfterBefore.put(IHTOPIC, record.topic());
        mapPayloadAfterBefore.put(IHPARTITION, record.kafkaPartition());
        mapPayloadAfterBefore.put(IHOFFSET, String.valueOf(record.kafkaOffset()));
        mapPayloadAfterBefore.put(IHOP, String.valueOf(mapValue.get(OP)));

        var mapCaseInsensitive = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        mapCaseInsensitive.putAll(mapPayloadAfterBefore);

        buffer.add(mapCaseInsensitive);
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {

        if (buffer.isEmpty()) {
            return;
        }

        var destFileName = UUID.randomUUID().toString();
        try {
            LOGGER.debug("Preparing to send {} records from buffer. To stage {} and table {}", buffer.size(), stageName,
                    tableName);

            /*
             * truncate if truncateBeforeFlush is true and last flush was more than 30
             * minutes ago
             */
            var minutesLastFlush = ChronoUnit.MINUTES.between(lastFlush, LocalDateTime.now());
            if (truncateBeforeBulk && minutesLastFlush > (truncateWhenNoDataAfterSeconds / 60)) {
                try (var stmt = connection.createStatement()) {
                    String truncateTable = String.format("TRUNCATE TABLE %s", tableName);
                    LOGGER.debug("Truncating table: {}", truncateTable);
                    stmt.executeLargeUpdate(truncateTable);
                } catch (Throwable e) {
                    LOGGER.error("Error while truncating table", e);
                    throw new RuntimeException("Error while truncating table", e);
                }
            }

            var lines = prepareOrderedColumnsBasedOnTargetTable(columnsFinalTable);
            try (var csvToInsert = createCsvFile(lines);
                 var inputStream = new ByteArrayInputStream(csvToInsert.toByteArray())) {
                snowflakeConnection.uploadStream(stageName, "/", inputStream,
                        destFileName, true);

            }catch (Throwable e) {
                LOGGER.error("Error while creating csv file or uploading to stage", e);
                throw new RuntimeException("Error while creating csv file or uploading to stage", e);
            } finally {
                // clear buffer and lines after upload so we avoid wasting memory
                buffer.clear();
                lines.clear();
            }


            var stmt = connection.createStatement();
            String copyInto = String.format("COPY INTO %s (%s) FROM @%s/%s.gz %s", tableName, String.join(",", columnsFinalTable),
                    stageName, destFileName, removeStageAfterCopy ? "PURGE = TRUE" : "");
            LOGGER.debug("Copying statement: {}", copyInto);
            stmt.executeLargeUpdate(copyInto);

        } catch (Throwable e) {
            LOGGER.error("Error while flushing Snowflake connector", e);
            throw new RuntimeException("Error while flushing", e);
        } finally {
            buffer.clear();
            lastFlush = LocalDateTime.now();
        }
    }

    @Override
    public void stop() {
        if (scheduler != null) {
            try {
                scheduler.shutdown();
            } catch (SchedulerException e) {
                LOGGER.error("Can not shutdown quartz scheduler", e);
            }
        }
    }

    private void validateFieldOnMap(String fieldToValidate, Map<String, ?> map) {
        if (!map.containsKey(fieldToValidate) || map.get(fieldToValidate) == null) {
            LOGGER.error("Key [{}] is missing or null on json", fieldToValidate);
            throw new RuntimeException("missing or null key [" + fieldToValidate + "] on json");
        }
    }

    private ByteArrayOutputStream createCsvFile(List<String> lines) {
        var csvInMemory = new ByteArrayOutputStream();
        for (var line : lines) {
            csvInMemory.writeBytes(line.getBytes(StandardCharsets.UTF_8));
            csvInMemory.writeBytes("\n".getBytes());
        }
        return csvInMemory;
    }

    private List<String> prepareOrderedColumnsBasedOnTargetTable(List<String> columnsFromTable) {

        var lines = new ArrayList<String>();

        boolean loggedDebugForFirstLine = false;
        for (var recordInBuffer : buffer) {
            var fullLine = new StringBuilder();
            for (int i = 0; i < columnsFromTable.size(); i++) {
                var columnFromSnowflakeTable = columnsFromTable.get(i);
                if (recordInBuffer.containsKey(columnFromSnowflakeTable)) {
                    var valueFromRecord = recordInBuffer.get(columnFromSnowflakeTable);

                    if (valueFromRecord != null) {
                        if (containsAny(columnFromSnowflakeTable, timestampFieldsConvertToSeconds)){
                            var valueFromRecordAsLong = (long) valueFromRecord;
                            valueFromRecord = LocalDateTime.ofInstant(Instant.ofEpochMilli(valueFromRecordAsLong),
                                    TimeZone.getDefault().toZoneId()).toString();
                        }else if (containsAny(columnFromSnowflakeTable, dateFieldsConvert)) {
                            var valueFromRecordAsLong = (long) valueFromRecord;
                            var daysInSeconds = valueFromRecordAsLong * 24 * 60 * 60;
                            valueFromRecord = LocalDate.ofInstant(Instant.ofEpochSecond(daysInSeconds),
                                    TimeZone.getDefault().toZoneId()).toString();
                        }else if (containsAny(columnFromSnowflakeTable, timeFieldsConvert)) {
                            var valueFromRecordAsLong = (long) valueFromRecord;
                            valueFromRecord = LocalTime.ofNanoOfDay(valueFromRecordAsLong).toString();
                        }
                    }

                    if (valueFromRecord != null) {
                        valueFromRecord = valueFromRecord.toString().replaceAll("\"", "\"\"");
                        fullLine.append("\"").append(valueFromRecord).append("\"");
                    }
                } else {
                    LOGGER.warn("Column {} not found on buffer, inserted empty value", columnFromSnowflakeTable);
                }

                if (i < columnsFromTable.size() - 1) {
                    fullLine.append(",");
                }
            }

            lines.add(fullLine.toString());

            if (!loggedDebugForFirstLine && LOGGER.isDebugEnabled()) {
                LOGGER.debug("csv line: {}", fullLine);
                loggedDebugForFirstLine = true;
            }
        }

        return lines;
    }

    private boolean containsAny(String checkValue, List<String> values) {
        for (String s : values) {
            if (s.trim().equalsIgnoreCase(checkValue.trim())) {
                return true;
            }
        }

        return false;
    }

    private List<String> getColumnsFromMetadata(String table) throws SQLException {
        var metadata = connection.getMetaData();

        var columnsFromTable = new ArrayList<String>();
        try (var rsColumns = metadata.getColumns(null, schemaName.toUpperCase(), table.toUpperCase(), null)) {
            while (rsColumns.next()) {
                columnsFromTable.add(rsColumns.getString("COLUMN_NAME").toUpperCase());
            }
        }
        if (columnsFromTable.isEmpty()) {
            throw new RuntimeException(
                    "Empty columns returned from target table " + table + ", schema " + schemaName);
        }

        columnsFromTable.removeAll(ignoreColumns);

        LOGGER.debug("Columns mapped from target table: {}", String.join(",", columnsFromTable));

        return columnsFromTable;
    }
}