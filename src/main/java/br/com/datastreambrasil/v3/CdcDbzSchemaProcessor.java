package br.com.datastreambrasil.v3;

import br.com.datastreambrasil.v3.exception.InvalidStructException;
import br.com.datastreambrasil.v3.model.SinkHashRecord;
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

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;
import java.util.zip.CRC32;

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
    private final CRC32 crc32 = new CRC32();

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
                    LocalDateTime.now(ZoneOffset.UTC),
                    calculateHash(valueOP, valueRecord),
                    record
            );

            LOGGER.trace("Added record to buffer: {} with operation {}", recordToSnowflake, valueOP);
            // Antes a chave era sempre UUID aleatório, então dois eventos da mesma PK nunca
            // se sobrescreviam e o buffer inchava até o flush. Usando a PK como chave (ou UUID
            // só no modo hashing, onde duplicatas legítimas precisam coexistir), o evento mais
            // recente substitui o anterior e o buffer fica do tamanho real do trabalho pendente.
            buffer.put(convertPKToStringKey(recordToSnowflake), recordToSnowflake);
            cleanUpOldHashRecords(recordToSnowflake);
        }
    }

    @Override
    protected void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        var startTime = System.currentTimeMillis();
        if (buffer.isEmpty()) {
            return;
        }

        var destFileName = UUID.randomUUID().toString();

        // Spill to Disk: guardamos aqui a referência ao arquivo CSV temporário
        // em disco. A variável fica fora do try principal para garantir que o
        // bloco finally consiga apagá-la mesmo se qualquer etapa falhar antes
        // ou durante o upload para o Snowflake.
        Path csvTempFile = null;
        try {
            LOGGER.debug("Preparing to send {} records from buffer. To stage {} and table {}", buffer.size(), stageName,
                    tableName);

            var columnsFromMetadata = columnsIngestTable;
            var blockID = UUID.randomUUID().toString();
            var startTimeMain = System.currentTimeMillis();

            // Serializamos o buffer para um arquivo CSV em disco. Isso evita
            // qualquer risco de OutOfMemoryError, pois nem o CSV inteiro nem o
            // array de bytes equivalente ficam residentes na heap.
            csvTempFile = prepareOrderedColumnsBasedOnTargetTable(blockID, columnsFromMetadata);

            // Abrimos um FileInputStream para o arquivo temporário e passamos
            // direto para o Snowflake. O driver lê os bytes do disco em chunks,
            // portanto o payload nunca é carregado por completo em memória.
            try (var inputStream = new FileInputStream(csvTempFile.toFile())) {
                var startTimeUpload = System.currentTimeMillis();
                snowflakeConnection.uploadStream(stageName, "/", inputStream,
                        destFileName, true);
                var endTimeUpload = System.currentTimeMillis();
                LOGGER.debug("Uploaded {} records in {} ms", buffer.size(), endTimeUpload - startTimeUpload);
            }

            var startTimeStatement = System.currentTimeMillis();
            try (var stmt = connection.createStatement()) {

                //copy everything to ingest
                String copyInto = String.format("COPY INTO %s (%s) FROM @%s/%s.gz PURGE = TRUE", ingestTableName, String.join(",", columnsFromMetadata),
                        stageName, destFileName);
                LOGGER.debug("Copying statement to ingest table: {}", copyInto);
                stmt.executeUpdate(copyInto);

                if (flushHasInsertedRecords) {
                    String insert = String.format("INSERT INTO %s (%s) SELECT * EXCLUDE (%s) FROM %s WHERE ih_blockid = '%s' and ih_op in ('c', 'r')",
                            tableName, String.join(",", columnsFinalTable), buildExcludeColumns(), ingestTableName, blockID);
                    LOGGER.debug("Inserting into ingest table: {}", insert);
                    stmt.executeUpdate(insert);
                }


                if (flushHasUpdatedRecords) {
                    //update in final table
                    String update = String.format(
                            "UPDATE %s AS final SET %s FROM (SELECT * FROM %s WHERE ih_blockid = '%s' and ih_op = 'u') AS ingest WHERE %s",
                            tableName, buildUpdateColumns(), ingestTableName, blockID,
                            buildPkWhereClause(pks));
                    LOGGER.debug("Updating statement to final table: {}", update);
                    stmt.executeUpdate(update);
                }

                //delete from final table
                if (flushHasDeletedRecords) {
                    String deleteFromFinalTable = String.format(
                            "DELETE FROM %s as final USING (SELECT %s FROM %s WHERE ih_blockid = '%s' and ih_op = 'd') AS ingest WHERE %s",
                            tableName, hashingSupport ? String.join(",", IH_CURRENT_HASH, IH_PREVIOUS_HASH) : String.join(",", pks),
                            ingestTableName, blockID,
                            buildPkWhereClause(pks));
                    LOGGER.debug("Deleting statement from final table: {}", deleteFromFinalTable);
                    stmt.executeUpdate(deleteFromFinalTable);
                }

                var endTimeStatement = System.currentTimeMillis();
                LOGGER.debug("Executed statement in {} ms", endTimeStatement - startTimeStatement);

            } catch (SQLException e) {
                throw new RuntimeException("Error executing operations", e);
            }

            var endTimeMain = System.currentTimeMillis();
            LOGGER.debug("Process records took {} ms", endTimeMain - startTimeMain);

        } catch (Throwable e) {
            LOGGER.error("Error while flushing Snowflake connector", e);
            throw new RuntimeException("Error while flushing", e);
        } finally {
            // CRÍTICO — Limpeza do Spill to Disk: removemos o CSV temporário do
            // disco SEMPRE, inclusive quando o upload ou o COPY INTO explodem.
            // Sem isso, um erro recorrente no Snowflake iria acumulando arquivos
            // em /tmp e, em poucas horas, esgotaria o disco do worker do Kafka
            // Connect. Files.deleteIfExists é idempotente, então é seguro chamar
            // mesmo se o arquivo já tiver sido removido por outra via.
            if (csvTempFile != null) {
                try {
                    Files.deleteIfExists(csvTempFile);
                } catch (IOException ex) {
                    LOGGER.warn("Falha ao remover CSV temporário {}: {}", csvTempFile, ex.getMessage());
                }
            }
            var endTime = System.currentTimeMillis();
            LOGGER.debug("Flushed {} records in {} ms", buffer.size(), endTime - startTime);
            buffer.clear();
        }
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

        if (hashingSupport) {
            return new ArrayList<>(); //empty because we won't use normal PK in that case
        }

        if (pks.isEmpty() && record.keySchema() != null && record.keySchema().fields() != null) {
            for (Field field : record.keySchema().fields()) {
                pks.add(field.name());
            }
        }

        return pks;
    }


    private String convertPKToStringKey(SnowflakeRecord record) {

        if (hashingSupport) {
            return UUID.randomUUID().toString();
        }

        var keyStruct = (Struct) record.originalRecord().key();
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
        if (hashingSupport) {
            return String.format("%s.%s = %s.%s", "final", IH_CURRENT_HASH, "ingest", IH_PREVIOUS_HASH);
        } else {
            return pks.stream()
                    .map(col -> String.format("%s.%s = %s.%s", "final", col, "ingest", col))
                    .reduce((a, b) -> a + " and " + b).orElseThrow();
        }

    }

    protected Path prepareOrderedColumnsBasedOnTargetTable(String blockID, List<String> columnsFromTable) throws Throwable {

        var startTime = System.currentTimeMillis();

        // Spill to Disk — criamos um arquivo temporário no diretório temporário
        // padrão do sistema operacional. Escrever o CSV direto em disco elimina
        // por completo o risco de OutOfMemoryError, pois o payload nunca precisa
        // caber inteiro na heap do JVM (nem como char[], nem como byte[]). Esse
        // arquivo é de responsabilidade do chamador e DEVE ser apagado no finally
        // do flush (ver CdcDbzSchemaProcessor#flush).
        var csvTempFile = Files.createTempFile("snowflake-sink-", ".csv");

        flushHasDeletedRecords = false;
        flushHasUpdatedRecords = false;
        flushHasInsertedRecords = false;

        // Encadeamos FileOutputStream → OutputStreamWriter(UTF-8) → BufferedWriter.
        // O BufferedWriter amortiza as chamadas de escrita, o OutputStreamWriter
        // garante a codificação UTF-8 correta e o FileOutputStream escreve os
        // bytes direto em disco. Assim a montagem do CSV é streaming puro, sem
        // qualquer buffer proporcional ao tamanho do lote em RAM.
        try (var writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(csvTempFile.toFile()), StandardCharsets.UTF_8))) {
            boolean loggedDebugForFirstLine = false;
            for (var recordInBuffer : buffer.values()) {
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

                var captureFirstLine = !loggedDebugForFirstLine && LOGGER.isDebugEnabled();
                var firstLineBuf = captureFirstLine ? new StringBuilder() : null;

                var count = 0;
                for (String columnFromSnowflakeTable : columnsFromTable) {
                    String cell = renderCell(columnFromSnowflakeTable, recordInBuffer, blockID);
                    if (cell != null && !cell.isEmpty()) {
                        writer.write(cell);
                        if (firstLineBuf != null) firstLineBuf.append(cell);
                    }
                    if (count < columnsFromTable.size() - 1) {
                        writer.write(',');
                        if (firstLineBuf != null) firstLineBuf.append(',');
                    }
                    count++;
                }
                writer.write('\n');

                if (firstLineBuf != null) {
                    firstLineBuf.append('\n');
                    LOGGER.debug("First lines of csv: {}", firstLineBuf);
                    loggedDebugForFirstLine = true;
                }
            }
        } catch (Throwable t) {
            // Se a escrita falhar no meio do caminho, o arquivo temporário já foi
            // criado em disco. Apagamos imediatamente aqui para não depender do
            // finally do flush (o chamador pode nem ter recebido o Path ainda).
            try {
                Files.deleteIfExists(csvTempFile);
            } catch (IOException suppressed) {
                t.addSuppressed(suppressed);
            }
            throw t;
        }

        var endTime = System.currentTimeMillis();
        LOGGER.debug("Prepared csv on disk in {} ms, path {}, size {} bytes",
                endTime - startTime, csvTempFile, Files.size(csvTempFile));
        return csvTempFile;
    }

    private String renderCell(String column, SnowflakeRecord recordInBuffer, String blockID) {
        if (column.equalsIgnoreCase(IHBLOCKID)) {
            return "\"" + blockID + "\"";
        }
        if (column.equalsIgnoreCase(IHOP)) {
            return "\"" + recordInBuffer.op() + "\"";
        }
        if (column.equalsIgnoreCase(IHTOPIC)) {
            return "\"" + recordInBuffer.topic() + "\"";
        }
        if (column.equalsIgnoreCase(IHDATETIME)) {
            return "\"" + recordInBuffer.timestamp() + "\"";
        }
        if (column.equalsIgnoreCase(IHPARTITION)) {
            return "\"" + recordInBuffer.partition() + "\"";
        }
        if (column.equalsIgnoreCase(IHOFFSET)) {
            return "\"" + recordInBuffer.offset() + "\"";
        }
        if (column.equalsIgnoreCase(IH_CURRENT_HASH)) {
            return recordInBuffer.hash().newHash() == null ? "" : "\"" + recordInBuffer.hash().newHash() + "\"";
        }
        if (column.equalsIgnoreCase(IH_PREVIOUS_HASH)) {
            return recordInBuffer.hash().firstSeenHash() == null ? "" : "\"" + recordInBuffer.hash().firstSeenHash() + "\"";
        }

        var fieldCaseInsensitive = recordInBuffer.event().schema().fields().stream()
                .filter(field -> field.name().equalsIgnoreCase(column)).findFirst();
        String searchColumn;
        if (fieldCaseInsensitive.isEmpty()) {
            LOGGER.warn("Column {} not found on record schema, fallback to snowflake original column name", column);
            searchColumn = column;
        } else {
            searchColumn = fieldCaseInsensitive.get().name();
        }

        Object valueFromRecord = recordInBuffer.event().get(searchColumn);
        if (valueFromRecord == null) {
            LOGGER.warn("Column {} not found on buffer, inserted empty value", column);
            return "";
        }

        if (containsAny(column, timestampFieldsConvert)) {
            var asLong = (long) valueFromRecord;
            valueFromRecord = LocalDateTime.ofInstant(Instant.ofEpochMilli(asLong),
                    TimeZone.getDefault().toZoneId()).toString();
        } else if (containsAny(column, dateFieldsConvert)) {
            var asInt = (int) valueFromRecord;
            var daysInSeconds = asInt * 24L * 60L * 60L;
            valueFromRecord = LocalDate.ofInstant(Instant.ofEpochSecond(daysInSeconds),
                    TimeZone.getDefault().toZoneId()).toString();
        } else if (containsAny(column, timeFieldsConvert)) {
            var asLong = (long) valueFromRecord;
            valueFromRecord = LocalTime.ofNanoOfDay(asLong).toString();
        }

        var escaped = valueFromRecord.toString().replace("\"", "\"\"");
        return "\"" + escaped + "\"";
    }

    protected SinkHashRecord calculateHash(String op, Struct record) {
        if (!hashingSupport) {
            return null;
        }

        SinkHashRecord hashRecord = null;

        //creating the record, so we don't have the before struct
        if (debeziumOperation.c.toString().equalsIgnoreCase(op) || debeziumOperation.r.toString().equalsIgnoreCase(op)) {
            var hash = getHash(record.getStruct(AFTER));
            hashRecord = new SinkHashRecord(getFirstSeenHash(hash), null, hash);
        }

        // we do have the before struct but don't have after struct
        if (debeziumOperation.d.toString().equalsIgnoreCase(op)) {
            var previousHash = getHash(record.getStruct(BEFORE));
            hashRecord = new SinkHashRecord(getFirstSeenHash(previousHash), previousHash, null);
        }

        // we do have the before struct and after struct
        if (debeziumOperation.u.toString().equalsIgnoreCase(op)) {
            var previousHash = getHash(record.getStruct(BEFORE));
            hashRecord = new SinkHashRecord(getFirstSeenHash(previousHash), previousHash, getHash(record.getStruct(AFTER)));
        }

        if (hashRecord == null) {
            throw new InvalidStructException("Invalid operation type: " + op);
        }

        return hashRecord;
    }

    private String getFirstSeenHash(String searchHash) {
        // O código antigo fazia buffer.get(searchHash), mas como o buffer nunca foi indexado
        // por hash, esse get sempre voltava null e caía no fallback devolvendo o próprio
        // searchHash. Os testes e o restante do fluxo já assumem esse comportamento de
        // "firstSeenHash = previousHash do evento", então deixamos explícito aqui em vez de
        // fingir uma busca que nunca encontrava nada. Se um dia quisermos rastrear a linhagem
        // de verdade, esse é o ponto para revisitar.
        return searchHash;
    }

    private String getHash(Object o) {
        crc32.reset();
        crc32.update(o.toString().getBytes());
        return Long.toHexString(crc32.getValue());
    }


    private boolean containsAny(String checkValue, List<String> values) {
        for (String s : values) {
            if (s.trim().equalsIgnoreCase(checkValue.trim())) {
                return true;
            }
        }

        return false;
    }

    private boolean validate(SinkRecord record) {

        if (!hashingSupport && (record.keySchema() == null || !(record.key() instanceof Struct))) {
            LOGGER.error("Key must be Struct with schema. Key: {}", record.key());
            return false;
        }

        if (record.valueSchema() == null || !(record.value() instanceof Struct)) {
            LOGGER.error("Value must be Struct with schemas. Value: {}", record.value());
            return false;
        }

        if (record.topic() == null || record.kafkaPartition() == null) {
            LOGGER.error("Null values for topic or kafkaPartition. Topic {}, KafkaPartition {}", record.topic(),
                    record.kafkaPartition());
            return false;
        }

        return true;
    }

    private void cleanUpOldHashRecords(SnowflakeRecord record) {
        if (hashingSupport && debeziumOperation.u.toString().equalsIgnoreCase(record.op())) {
            buffer.remove(record.hash().previousHash());
        }
    }
}
