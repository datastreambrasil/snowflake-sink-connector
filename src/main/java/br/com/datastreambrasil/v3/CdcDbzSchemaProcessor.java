package br.com.datastreambrasil.v3;

import br.com.datastreambrasil.v3.compress.CompressedMap;
import br.com.datastreambrasil.v3.exception.InvalidStructException;
import br.com.datastreambrasil.v3.model.FieldRecord;
import br.com.datastreambrasil.v3.model.SnowflakeRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;

/**
 * CdcDbzSchemaProcessor receives SinkRecords using Struct with Schema in CDC Debezium format.
 * This processor will process all operations (snapshot,insert, update, delete) and save on snowflake.
 * Supports multiple tables per DB/schema: the table name is extracted from the topic (value after the last dot).
 */
public class CdcDbzSchemaProcessor extends AbstractProcessor {

    private Scheduler scheduler;
    private Map<String, List<String>> pksByTable = new HashMap<>();
    private boolean flushHasDeletedRecords;
    private boolean flushHasInsertedRecords;
    private boolean flushHasUpdatedRecords;

    @Override
    protected void extraConfigsOnStart(AbstractConfig config) {
        try {
            startCleanUpJob(config);
        } catch (SchedulerException e) {
            LOGGER.error("Error starting cleanup job", e);
            throw new RuntimeException("Error starting cleanup job", e);
        }
    }

    @Override
    protected void put(Collection<SinkRecord> collection) {
        LOGGER.debug("PUT - Total number of records to be stored in the buffer: {}", collection.size());
        for (SinkRecord record : collection) {

            if (!validate(record)) {
                throw new InvalidStructException("Invalid record structure or schema");
            }

            String tableBaseName = extractTableNameFromTopic(record.topic());
            List<String> tablePks = extractPK(record, tableBaseName);

            var fieldOP = record.valueSchema().field(OP);
            if (fieldOP == null) {
                LOGGER.error("Field '{}' not found in value schema for record: {}", OP, record);
                throw new InvalidStructException("Field '" + OP + "' not found in value schema");
            }

            var valueRecord = (Struct) record.value();
            var valueOP = valueRecord.getString(fieldOP.name());
            if (valueOP == null) {
                LOGGER.error("Value for field '{}' is null in record: {}", OP, record);
                throw new InvalidStructException("Value for field '" + OP + "' is null");
            }

            var recordToSnowflake = new SnowflakeRecord(
                    this.prepareEvent(debeziumOperation.d.toString().equalsIgnoreCase(valueOP) ? valueRecord.getStruct(BEFORE) : valueRecord.getStruct(AFTER)),
                    record.topic(),
                    record.kafkaPartition(),
                    record.kafkaOffset(),
                    valueOP,
                    LocalDateTime.now(ZoneOffset.UTC)
            );

            LOGGER.trace("Added record to buffer [table={}]: {} with operation {}", tableBaseName, recordToSnowflake, valueOP);
            getOrCreateBuffer(tableBaseName).put(convertPKToStringKey(record, tablePks), recordToSnowflake);
        }
    }

    private List<FieldRecord> prepareEvent(Struct struct) {
        var fields = new ArrayList<FieldRecord>(struct.schema().fields().size());
        struct.schema().fields().forEach(fs -> fields.add(new FieldRecord(fs.name(), struct.get(fs.name()))));
        return fields;
    }

    private boolean validate(SinkRecord record) {
        if (record.keySchema() == null || record.valueSchema() == null ||
                !(record.key() instanceof Struct) || !(record.value() instanceof Struct)) {
            LOGGER.error("Key and value must be Structs with schemas. Key: {}, Value: {}", record.key(), record.value());
            return false;
        }

        if (record.topic() == null || record.kafkaPartition() == null) {
            LOGGER.error("Null values for topic or kafkaPartition. Topic {}, KafkaPartition {}", record.topic(),
                    record.kafkaPartition());
            return false;
        }

        return true;
    }

    @Override
    protected void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        for (var entry : buffer.entrySet()) {
            var tableBaseName = entry.getKey();
            var tableBuffer = entry.getValue();

            if (!tableBuffer.isEmpty()) {
                flushTable(tableBaseName, tableBuffer);
            }
            buffer.remove(tableBaseName);
        }
    }

    private void flushTable(String tableBaseName, CompressedMap<SnowflakeRecord> tableBuffer) {
        var startTime = System.currentTimeMillis();
        var totalBuffer = tableBuffer.size();
        var ingestTableName = tableBaseName + INGEST_SUFFIX;
        var destFileName = UUID.randomUUID().toString();
        Path tmpFilePathToInsert = null;

        try {
            LOGGER.debug("Preparing to send {} records from buffer. To stage {} and table {}", tableBuffer.size(), stageName, tableBaseName);

            ensureColumnsForTable(tableBaseName);
            var ingestCols = columnsIngestTable.get(tableBaseName);
            var finalCols = columnsFinalTable.get(tableBaseName);
            var tablePks = pksByTable.get(tableBaseName);
            var blockID = UUID.randomUUID().toString();
            var startTimeMain = System.currentTimeMillis();
            tmpFilePathToInsert = prepareOrderedColumnsBasedOnTargetTable(blockID, ingestCols, tableBuffer);

            try (var inputStream = Files.newInputStream(tmpFilePathToInsert)) {
                var startTimeUpload = System.currentTimeMillis();
                snowflakeConnection.uploadStream(stageName, "/", inputStream, destFileName, true);
                var endTimeUpload = System.currentTimeMillis();
                LOGGER.debug("Uploaded {} records in {} ms", tableBuffer.size(), endTimeUpload - startTimeUpload);

                var startTimeStatement = System.currentTimeMillis();
                try (var stmt = connection.createStatement()) {

                    String copyInto = String.format("COPY INTO %s (%s) FROM @%s/%s.gz PURGE = TRUE", ingestTableName,
                            String.join(",", ingestCols), stageName, destFileName);
                    LOGGER.debug("Copying statement to ingest table: {}", copyInto);
                    stmt.executeLargeUpdate(copyInto);

                    this.discardData(tmpFilePathToInsert);

                    if (flushHasInsertedRecords || flushHasUpdatedRecords) {
                        String merge = String.format("MERGE INTO %s AS final USING (SELECT * EXCLUDE (%s) FROM %s WHERE ih_blockid = '%s' and ih_op in ('c', 'r', 'u')) AS ingest ON %s " +
                                        "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s) " +
                                        "WHEN MATCHED THEN UPDATE SET %s",
                                tableBaseName, buildExcludeColumns(), ingestTableName, blockID,
                                buildPkWhereClause(tablePks), String.join(",", finalCols),
                                String.join(",", finalCols.stream().map(c -> "ingest." + c).toList()),
                                buildUpdateColumns(finalCols));
                        LOGGER.debug("Merging statement to final table: {}", merge);
                        stmt.executeLargeUpdate(merge);
                    }

                    if (flushHasDeletedRecords) {
                        String deleteFromFinalTable = String.format(
                                "DELETE FROM %s as final USING (SELECT %s FROM %s WHERE ih_blockid = '%s' and ih_op = 'd') AS ingest WHERE %s",
                                tableBaseName, String.join(",", tablePks), ingestTableName, blockID,
                                buildPkWhereClause(tablePks));
                        LOGGER.debug("Deleting statement from final table: {}", deleteFromFinalTable);
                        stmt.executeLargeUpdate(deleteFromFinalTable);
                    }

                    var endTimeStatement = System.currentTimeMillis();
                    LOGGER.debug("Executed statement in {} ms", endTimeStatement - startTimeStatement);

                } catch (SQLException e) {
                    throw new RuntimeException("Error executing operations", e);
                }
            }
            var endTimeMain = System.currentTimeMillis();
            LOGGER.debug("Process records took {} ms", endTimeMain - startTimeMain);

        } catch (Throwable e) {
            LOGGER.error("Error while flushing Snowflake connector for table {}", tableBaseName, e);

            if (tmpFilePathToInsert != null) {
                try {
                    Files.deleteIfExists(tmpFilePathToInsert);
                } catch (IOException ex) {
                    LOGGER.warn("Temp file: {} not found after flushing Snowflake connector",
                            tmpFilePathToInsert.getFileName().toString(), ex);
                }
            }

            throw new RuntimeException("Error while flushing table " + tableBaseName, e);
        } finally {
            var endTime = System.currentTimeMillis();
            LOGGER.debug("Flushed {} records in {} ms", totalBuffer, endTime - startTime);
            tableBuffer.clear();
        }
    }

    private void discardData(Path csvToInsert) throws IOException {
        LOGGER.debug("Discard CSV file data: {} of stage: {} after SnowFlake upload and COPY to ingest table.",
                csvToInsert.getFileName().toString(), stageName);
        Files.deleteIfExists(csvToInsert);
        LOGGER.debug("Discarded CSV file data: {} of stage: {}.", csvToInsert.getFileName().toString(), stageName);
    }

    @Override
    protected void stop() {
        if (scheduler != null) {
            try {
                scheduler.shutdown();
            } catch (SchedulerException e) {
                LOGGER.error("Can not shutdown quartz scheduler", e);
            }
        }
    }

    protected void startCleanUpJob(AbstractConfig config) throws SchedulerException {

        var disableCleanUpJob = config.getBoolean(SnowflakeSinkConnector.CFG_JOB_CLEANUP_DISABLE);

        if (disableCleanUpJob) {
            LOGGER.warn("Cleanup job is disabled, skipping job creation.");
            return;
        }

        var durationCleanup = Duration.parse(config.getString(SnowflakeSinkConnector.CFG_JOB_CLEANUP_DURATION));
        LOGGER.info("Cleanup job will run every {} seconds", durationCleanup.toSeconds());

        var jobData = new HashMap<String, Object>();
        jobData.put(CleanupJob.SNOWFLAKE_CONNECTION, connection);
        jobData.put(CleanupJob.INGEST_TABLE_NAMES, knownIngestTables);

        var props = new Properties();
        props.setProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME, String.format("cleanup_%s", UUID.randomUUID()));
        props.setProperty("org.quartz.threadPool.threadCount", "1");

        var schedulerFactory = new StdSchedulerFactory(props);
        scheduler = schedulerFactory.getScheduler();
        var job = JobBuilder.newJob(CleanupJob.class).withIdentity("cleanupjob")
                .setJobData(new JobDataMap(jobData))
                .build();
        var trigger = TriggerBuilder.newTrigger().withIdentity("trigger_cleanupjob")
                .withSchedule(SimpleScheduleBuilder.repeatSecondlyForever((int) durationCleanup.getSeconds()))
                .build();
        scheduler.scheduleJob(job, trigger);
        scheduler.start();
    }

    private List<String> extractPK(SinkRecord record, String tableBaseName) {
        return pksByTable.computeIfAbsent(tableBaseName, k -> {
            var pks = new ArrayList<String>();
            for (Field field : record.keySchema().fields()) {
                pks.add(field.name());
            }
            return pks;
        });
    }

    private String convertPKToStringKey(SinkRecord record, List<String> pks) {
        var keyStruct = (Struct) record.key();
        var pkValues = new ArrayList<String>();
        for (String pk : pks) {
            var value = keyStruct.get(pk);
            if (value == null) {
                LOGGER.error("Value for field '{}' is null in record: {}", pk, record);
                throw new InvalidStructException("Value for field '" + pk + "' is null");
            }
            pkValues.add(value.toString());
        }

        return String.join("+", pkValues);
    }

    private String buildUpdateColumns(List<String> finalCols) {
        var columns = new ArrayList<String>();
        for (String column : finalCols) {
            columns.add(String.format("final.%s = ingest.%s", column, column));
        }
        return String.join(",", columns);
    }

    private String buildExcludeColumns() {
        return String.join(",", IHPARTITION, IHDATETIME, IHBLOCKID, IHOFFSET, IHOP, IHTOPIC);
    }

    private String buildPkWhereClause(List<String> pks) {
        return pks.stream()
                .map(col -> String.format("final.%s = ingest.%s", col, col))
                .reduce((a, b) -> String.format("%s and %s", a, b)).orElseThrow();
    }

    protected Path prepareOrderedColumnsBasedOnTargetTable(String blockID, List<String> columnsFromTable, CompressedMap<SnowflakeRecord> tableBuffer) throws Throwable {
        var startTime = System.currentTimeMillis();
        var stringBuilder = new StringBuilder(tableBuffer.size() * SB_CSV_INITIAL_SIZE);

        flushHasDeletedRecords = false;
        flushHasUpdatedRecords = false;
        flushHasInsertedRecords = false;

        boolean loggedDebugForFirstLine = false;
        for (var recordInBuffer : tableBuffer.values()) {
            var recordData = tableBuffer.deserializeValue(recordInBuffer);
            var count = 0;
            var op = recordData.op();

            if (debeziumOperation.d.toString().equalsIgnoreCase(op)) {
                flushHasDeletedRecords = true;
            }

            if (debeziumOperation.c.toString().equalsIgnoreCase(op) || debeziumOperation.r.toString().equalsIgnoreCase(op)) {
                flushHasInsertedRecords = true;
            }

            if (debeziumOperation.u.toString().equalsIgnoreCase(op)) {
                flushHasUpdatedRecords = true;
            }

            for (String columnFromSnowflakeTable : columnsFromTable) {
                if (columnFromSnowflakeTable.equalsIgnoreCase(IHBLOCKID)) {
                    stringBuilder.append(DOUBLE_QUOTE)
                            .append(blockID)
                            .append(DOUBLE_QUOTE);
                } else if (columnFromSnowflakeTable.equalsIgnoreCase(IHOP)) {
                    stringBuilder.append(DOUBLE_QUOTE)
                            .append(recordData.op())
                            .append(DOUBLE_QUOTE);
                } else if (columnFromSnowflakeTable.equalsIgnoreCase(IHTOPIC)) {
                    stringBuilder.append(DOUBLE_QUOTE)
                            .append(recordData.topic())
                            .append(DOUBLE_QUOTE);
                } else if (columnFromSnowflakeTable.equalsIgnoreCase(IHDATETIME)) {
                    stringBuilder.append(DOUBLE_QUOTE)
                            .append(recordData.timestamp())
                            .append(DOUBLE_QUOTE);
                } else if (columnFromSnowflakeTable.equalsIgnoreCase(IHPARTITION)) {
                    stringBuilder.append(DOUBLE_QUOTE)
                            .append(recordData.partition())
                            .append(DOUBLE_QUOTE);
                } else if (columnFromSnowflakeTable.equalsIgnoreCase(IHOFFSET)) {
                    stringBuilder.append(DOUBLE_QUOTE)
                            .append(recordData.offset())
                            .append(DOUBLE_QUOTE);
                } else {
                    Optional<FieldRecord> fieldRecord = recordData.event().stream().filter(field ->
                            field.name().equalsIgnoreCase(columnFromSnowflakeTable)).findFirst();
                    if (fieldRecord.isPresent() && fieldRecord.get().data() != null) {
                        Object valueFromRecord = fieldRecord.get().data();
                        if (containsAny(columnFromSnowflakeTable, timestampFieldsConvert)) {
                            var valueFromRecordAsLong = (long) valueFromRecord;
                            valueFromRecord = LocalDateTime.ofInstant(Instant.ofEpochMilli(valueFromRecordAsLong),
                                    TimeZone.getDefault().toZoneId()).toString();
                        } else if (containsAny(columnFromSnowflakeTable, dateFieldsConvert)) {
                            var valueFromRecordAsLong = (int) valueFromRecord;
                            var daysInSeconds = valueFromRecordAsLong * 24 * 60 * 60;
                            valueFromRecord = LocalDate.ofInstant(Instant.ofEpochSecond(daysInSeconds),
                                    TimeZone.getDefault().toZoneId()).toString();
                        } else if (containsAny(columnFromSnowflakeTable, timeFieldsConvert)) {
                            var valueFromRecordAsLong = (long) valueFromRecord;
                            valueFromRecord = LocalTime.ofNanoOfDay(valueFromRecordAsLong).toString();
                        }

                        valueFromRecord = valueFromRecord.toString().replaceAll(DOUBLE_QUOTE, REGEX_REPLACEMENT_QUOTE_VALUE);
                        stringBuilder.append(DOUBLE_QUOTE).append(valueFromRecord).append(DOUBLE_QUOTE);
                    } else {
                        LOGGER.warn("Column {} not found on buffer, inserted empty value", columnFromSnowflakeTable);
                    }
                }

                if (count < columnsFromTable.size() - 1) {
                    stringBuilder.append(LINE_SEPARATOR_COMMA);
                }

                count++;
            }

            stringBuilder.append(BREAK_LINE);
            if (!loggedDebugForFirstLine && LOGGER.isDebugEnabled()) {
                LOGGER.debug("First lines of csv: {}", stringBuilder);
                loggedDebugForFirstLine = true;
            }
        }

        stringBuilder.trimToSize();

        return this.generateTempFile(stringBuilder, startTime);
    }

    private Path generateTempFile(StringBuilder stringBuilder, long startTime) throws IOException {
        Files.createDirectories(Path.of(String.format("%s/%s", tmpDataFolder, stageName)));

        var tmpPath = Path.of(String.format("%s/%s/%s.csv", tmpDataFolder, stageName, UUID.randomUUID()));
        Files.deleteIfExists(tmpPath);

        var resultPath = Files.writeString(tmpPath, stringBuilder.toString(), StandardOpenOption.CREATE);

        LOGGER.debug("Prepared csv in file in {} ms", System.currentTimeMillis() - startTime);

        stringBuilder.delete(0, stringBuilder.length());

        return resultPath;
    }

    private boolean containsAny(String checkValue, List<String> values) {
        return Optional.ofNullable(values).orElse(List.of()).stream()
                .anyMatch(s -> s.trim().equalsIgnoreCase(checkValue.trim()));
    }
}
