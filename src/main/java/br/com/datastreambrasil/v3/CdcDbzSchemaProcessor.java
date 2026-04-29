package br.com.datastreambrasil.v3;

import br.com.datastreambrasil.v3.exception.InvalidStructException;
import br.com.datastreambrasil.v3.model.SnowflakeRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.*;
import java.util.*;

/**
 * CdcDbzSchemaProcessor receives SinkRecords using Struct with Schema in CDC Debezium format.
 * This processor will process all operations (snapshot,insert, update, delete) and save on snowflake.
 */
public class CdcDbzSchemaProcessor extends AbstractProcessor {

    private Scheduler scheduler;
    private List<String> pks = new ArrayList<>();
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
        for (SinkRecord record : collection) {

            if (!validate(record)) {
                throw new InvalidStructException("Invalid record structure or schema");
            }

            pks = extractPK(record);

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
                    debeziumOperation.d.toString().equalsIgnoreCase(valueOP) ? valueRecord.getStruct(BEFORE) : valueRecord.getStruct(AFTER),
                    record.topic(),
                    record.kafkaPartition(),
                    record.kafkaOffset(),
                    valueOP,
                    LocalDateTime.now(ZoneOffset.UTC)
            );

            LOGGER.trace("Added record to buffer: {} with operation {}", recordToSnowflake, valueOP);
            buffer.put(convertPKToStringKey(record), recordToSnowflake);
        }
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
        var startTime = System.currentTimeMillis();
        if (buffer.isEmpty()) {
            return;
        }

        var destFileName = UUID.randomUUID().toString();
        try {
            LOGGER.debug("Preparing to send {} records from buffer. To stage {} and table {}", buffer.size(), stageName,
                    tableName);

            var columnsFromMetadata = columnsIngestTable;
            var blockID = UUID.randomUUID().toString();
            var startTimeMain = System.currentTimeMillis();
            try (var csvToInsert = prepareOrderedColumnsBasedOnTargetTable(blockID, columnsFromMetadata);
                 var inputStream = new ByteArrayInputStream(csvToInsert.toByteArray())) {

                var startTimeUpload = System.currentTimeMillis();
                snowflakeConnection.uploadStream(stageName, "/", inputStream,
                        destFileName, true);
                var endTimeUpload = System.currentTimeMillis();
                LOGGER.debug("Uploaded {} records in {} ms", buffer.size(), endTimeUpload - startTimeUpload);

                var startTimeStatement = System.currentTimeMillis();
                try (var stmt = connection.createStatement()) {

                    //copy everything to ingest
                    String copyInto = String.format("COPY INTO %s (%s) FROM @%s/%s.gz PURGE = TRUE", ingestTableName, String.join(",", columnsFromMetadata),
                            stageName, destFileName);
                    LOGGER.debug("Copying statement to ingest table: {}", copyInto);
                    stmt.executeUpdate(copyInto);

                    // Resets the CSV ByteArray to zero, so that all currently accumulated stream is discarded after SnowFlake upload and COPY to ingest table.
                    this.discardCsvData(inputStream, csvToInsert, destFileName, stageName);

                    if (flushHasInsertedRecords || flushHasUpdatedRecords) {
                        //insert/update in final table
                        String merge = String.format("MERGE INTO %s AS final USING (SELECT * EXCLUDE (%s) FROM %s WHERE ih_blockid = '%s' and ih_op in ('c', 'r', 'u')) AS ingest ON %s " +
                                        "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s) " +
                                        "WHEN MATCHED THEN UPDATE SET %s",
                                tableName, buildExcludeColumns(), ingestTableName, blockID,
                                buildPkWhereClause(pks), String.join(",", columnsFinalTable), String.join(",", columnsFinalTable.stream().map(c -> "ingest." + c).toList()),
                                buildUpdateColumns());
                        LOGGER.debug("Merging statement to final table: {}", merge);
                        stmt.executeUpdate(merge);
                    }

                    //delete from final table
                    if (flushHasDeletedRecords) {
                        String deleteFromFinalTable = String.format(
                                "DELETE FROM %s as final USING (SELECT %s FROM %s WHERE ih_blockid = '%s' and ih_op = 'd') AS ingest WHERE %s",
                                tableName, String.join(",", pks), ingestTableName, blockID,
                                buildPkWhereClause(pks));
                        LOGGER.debug("Deleting statement from final table: {}", deleteFromFinalTable);
                        stmt.executeUpdate(deleteFromFinalTable);
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
            LOGGER.error("Error while flushing Snowflake connector", e);
            throw new RuntimeException("Error while flushing", e);
        } finally {
            var endTime = System.currentTimeMillis();
            LOGGER.debug("Flushed {} records in {} ms", buffer.size(), endTime - startTime);
            buffer.clear();
        }
    }

    private void discardCsvData(ByteArrayInputStream inputStream, ByteArrayOutputStream csvToInsert, String destFileName, String stageName) {
        LOGGER.debug("Discard CSV byte array data: {} of stage: {} after SnowFlake upload and COPY to ingest table.", destFileName, stageName);
        inputStream.reset();
        csvToInsert.reset();
        LOGGER.debug("Discarded CSV byte array data: {} of stage: {}. CSV Data is {}", destFileName, stageName, csvToInsert.size());
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

        // job quartz config
        var jobData = new HashMap<String, Object>();
        jobData.put(CleanupJob.SNOWFLAKE_CONNECTION, connection);
        jobData.put(CleanupJob.INGEST_TABLE_NAME, ingestTableName);

        var props = new Properties();
        props.setProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME, "cleanup_" + UUID.randomUUID());
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

    private List<String> extractPK(SinkRecord record) {
        if (pks.isEmpty()) {
            for (Field field : record.keySchema().fields()) {
                pks.add(field.name());
            }
        }

        return pks;
    }


    private String convertPKToStringKey(SinkRecord record) {
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

    private String buildUpdateColumns() {
        var columns = new ArrayList<String>();
        for (String column : columnsFinalTable) {
            columns.add(String.format("final.%s = ingest.%s", column, column));
        }
        return String.join(",", columns);
    }

    private String buildExcludeColumns() {
        return String.join(",", IHPARTITION, IHDATETIME, IHBLOCKID, IHOFFSET, IHOP, IHTOPIC);
    }

    private String buildPkWhereClause(List<String> pks) {
        return pks.stream()
                .map(col -> String.format("%s.%s = %s.%s", "final", col, "ingest", col))
                .reduce((a, b) -> a + " and " + b).orElseThrow();
    }

    protected ByteArrayOutputStream prepareOrderedColumnsBasedOnTargetTable(String blockID, List<String> columnsFromTable) throws Throwable {

        var startTime = System.currentTimeMillis();
        var csvInMemory = new ByteArrayOutputStream();
        var stringBuilder = new StringBuilder();

        flushHasDeletedRecords = false;
        flushHasUpdatedRecords = false;
        flushHasInsertedRecords = false;

        boolean loggedDebugForFirstLine = false;
        for (var recordInBuffer : buffer.values()) {
            var count = 0;
            var op = recordInBuffer.op();

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
                    var strBuffer = "\"" + blockID + "\"";
                    stringBuilder.append(strBuffer);
                } else if (columnFromSnowflakeTable.equalsIgnoreCase(IHOP)) {
                    var strBuffer = "\"" + recordInBuffer.op() + "\"";
                    stringBuilder.append(strBuffer);
                } else if (columnFromSnowflakeTable.equalsIgnoreCase(IHTOPIC)) {
                    var strBuffer = "\"" + recordInBuffer.topic() + "\"";
                    stringBuilder.append(strBuffer);
                } else if (columnFromSnowflakeTable.equalsIgnoreCase(IHDATETIME)) {
                    var strBuffer = "\"" + recordInBuffer.timestamp() + "\"";
                    stringBuilder.append(strBuffer);
                } else if (columnFromSnowflakeTable.equalsIgnoreCase(IHPARTITION)) {
                    var strBuffer = "\"" + recordInBuffer.partition() + "\"";
                    stringBuilder.append(strBuffer);
                } else if (columnFromSnowflakeTable.equalsIgnoreCase(IHOFFSET)) {
                    var strBuffer = "\"" + recordInBuffer.offset() + "\"";
                    stringBuilder.append(strBuffer);
                } else {
                    var fieldCaseInsensitive = recordInBuffer.event().schema().fields().stream().filter(field ->
                            field.name().equalsIgnoreCase(columnFromSnowflakeTable)).findFirst();
                    String searchColumn;
                    if (fieldCaseInsensitive.isEmpty()) {
                        LOGGER.warn("Column {} not found on record schema, fallback to snowflake original column name", columnFromSnowflakeTable);
                        searchColumn = columnFromSnowflakeTable;
                    } else {
                        searchColumn = fieldCaseInsensitive.get().name();
                    }

                    Object valueFromRecord = recordInBuffer.event().get(searchColumn);
                    if (valueFromRecord != null) {
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

                        valueFromRecord = valueFromRecord.toString().replaceAll("\"", "\"\"");
                        var strBuffer = "\"" + valueFromRecord + "\"";
                        stringBuilder.append(strBuffer);

                    } else {
                        LOGGER.warn("Column {} not found on buffer, inserted empty value", columnFromSnowflakeTable);
                    }
                }

                if (count < columnsFromTable.size() - 1) {
                    stringBuilder.append(",");
                }

                count++;
            }


            stringBuilder.append("\n");
            if (!loggedDebugForFirstLine && LOGGER.isDebugEnabled()) {
                LOGGER.debug("First lines of csv: {}", stringBuilder);
                loggedDebugForFirstLine = true;
            }
        }

        if (!stringBuilder.isEmpty()) {
            csvInMemory.write(stringBuilder.toString().getBytes(StandardCharsets.UTF_8));
        }
        var endTime = System.currentTimeMillis();
        LOGGER.debug("Prepared csv in memory in {} ms", endTime - startTime);
        return csvInMemory;
    }


    private boolean containsAny(String checkValue, List<String> values) {
        for (String s : values) {
            if (s.trim().equalsIgnoreCase(checkValue.trim())) {
                return true;
            }
        }

        return false;
    }
}
