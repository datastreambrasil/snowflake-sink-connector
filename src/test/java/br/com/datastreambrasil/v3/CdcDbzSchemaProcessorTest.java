package br.com.datastreambrasil.v3;

import br.com.datastreambrasil.v3.exception.InvalidStructException;
import net.snowflake.client.jdbc.SnowflakeConnection;
import net.snowflake.client.jdbc.internal.apache.commons.io.IOUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.quartz.SchedulerException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static br.com.datastreambrasil.v3.AbstractProcessor.IHBLOCKID;
import static br.com.datastreambrasil.v3.AbstractProcessor.IHDATETIME;
import static br.com.datastreambrasil.v3.AbstractProcessor.IHOFFSET;
import static br.com.datastreambrasil.v3.AbstractProcessor.IHOP;
import static br.com.datastreambrasil.v3.AbstractProcessor.IHPARTITION;
import static br.com.datastreambrasil.v3.AbstractProcessor.IHTOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CdcDbzSchemaProcessorTest {

    private static Schema valueAfterBeforeSchema;
    private static Schema valueSchema;
    private static Schema keySchema;

    @BeforeAll
    static void beforeTest() {
        valueAfterBeforeSchema = SchemaBuilder.struct()
                .field("Id", Schema.STRING_SCHEMA)
                .field("Name", Schema.STRING_SCHEMA)
                .field("timestamp", Schema.OPTIONAL_INT64_SCHEMA)
                .field("time", Schema.OPTIONAL_INT64_SCHEMA)
                .field("date", Schema.OPTIONAL_INT32_SCHEMA)
                .field("desc", Schema.OPTIONAL_STRING_SCHEMA).build();
        valueSchema = SchemaBuilder.struct()
                .field("before", valueAfterBeforeSchema)
                .field("after", valueAfterBeforeSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();
        keySchema = SchemaBuilder.struct()
                .field("id", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    @Test
    void testPutSuccess() {
        var processor = new CdcDbzSchemaProcessor();
        processor.tableName = "test_table";
        processor.bufferInitialCapacity = 10;
        var dt = LocalDateTime.of(2025, 1, 20, 10, 30, 40);
        processor.put(generateCreateEvents(dt, "1", "2", "3"));
        processor.put(generateUpdateEvents(dt, "update 001", "10"));//this update should be ignored, because it will be overridden by the next update event
        processor.put(generateUpdateEvents(dt, "update 002", "10"));
        processor.put(generateDeleteEvents(dt, "20"));
        processor.put(generateDeleteEvents(dt, "3")); //this delete will override the create event for id 3

        var tableBuffer = processor.buffer.get("test_table");
        assertEquals(5, tableBuffer.size());

        var itemIdx = "1";
        //assert item create
        assertEquals(itemIdx, tableBuffer.get(itemIdx).event().stream()
                .filter(f -> f.name().equals("Id")).findFirst().get().data());
        assertEquals("Name " + itemIdx, tableBuffer.get(itemIdx).event().stream()
                .filter(f -> f.name().equals("Name")).findFirst().get().data());
        assertEquals("c", tableBuffer.get(itemIdx).op());

        //assert item update
        itemIdx = "10";
        assertEquals(itemIdx, tableBuffer.get(itemIdx).event().stream()
                .filter(f -> f.name().equals("Id")).findFirst().get().data());
        assertEquals("Name update 002 " + itemIdx, tableBuffer.get(itemIdx).event().stream()
                .filter(f -> f.name().equals("Name")).findFirst().get().data());
        assertEquals("u", tableBuffer.get(itemIdx).op());

        //asert item delete
        itemIdx = "20";
        assertEquals(itemIdx, tableBuffer.get(itemIdx).event().stream()
                .filter(f -> f.name().equals("Id")).findFirst().get().data());
        assertEquals("Name " + itemIdx, tableBuffer.get(itemIdx).event().stream()
                .filter(f -> f.name().equals("Name")).findFirst().get().data());
        assertEquals("d", tableBuffer.get(itemIdx).op());

        itemIdx = "3";
        assertEquals(itemIdx, tableBuffer.get(itemIdx).event().stream()
                .filter(f -> f.name().equals("Id")).findFirst().get().data());
        assertEquals("Name " + itemIdx, tableBuffer.get(itemIdx).event().stream()
                .filter(f -> f.name().equals("Name")).findFirst().get().data());
        assertEquals("d", tableBuffer.get(itemIdx).op());
    }

    @Test
    void testPutSuccess_MultipleTopics() {
        var processor = new CdcDbzSchemaProcessor();
        processor.tableName = "default_table";
        processor.bufferInitialCapacity = 10;
        var dt = LocalDateTime.of(2025, 1, 20, 10, 30, 40);

        // Records with dot-separated topics — table name extracted from last segment
        processor.put(generateCreateEventsForTopic(dt, "compras.loja-b.tabelaX", "1", "2"));
        processor.put(generateCreateEventsForTopic(dt, "compras.loja-c.tabelaZ", "3"));

        assertTrue(processor.buffer.containsKey("tabelaX"), "Buffer should contain tabelaX");
        assertTrue(processor.buffer.containsKey("tabelaZ"), "Buffer should contain tabelaZ");
        assertEquals(2, processor.buffer.get("tabelaX").size());
        assertEquals(1, processor.buffer.get("tabelaZ").size());
    }

    @Test
    void testPutSuccess_TopicWithoutDotFallsBackToTableName() {
        var processor = new CdcDbzSchemaProcessor();
        processor.tableName = "fallback_table";
        processor.bufferInitialCapacity = 10;
        var dt = LocalDateTime.of(2025, 1, 20, 10, 30, 40);

        processor.put(generateCreateEventsForTopic(dt, "no_dot_topic", "1"));

        assertTrue(processor.buffer.containsKey("fallback_table"), "Buffer should use tableName as fallback");
    }

    @Test
    void testPutFailWithInvalidValueSchema() {
        var processor = new CdcDbzSchemaProcessor();
        assertThrows(InvalidStructException.class, () -> processor.put(List.of(new SinkRecord(
                "test_topic",
                0,
                keySchema,
                new Struct(keySchema).put("id", "1"),
                valueSchema,
                "invalid_value", // This should be a Struct, but it's a String
                1
        ))));
    }

    @Test
    void testPutFailWithInvalidTopic() {
        var processor = new CdcDbzSchemaProcessor();
        assertThrows(InvalidStructException.class, () -> processor.put(List.of(new SinkRecord(
                null, // Invalid topic
                0,
                keySchema,
                new Struct(keySchema).put("id", "1"),
                valueSchema,
                new Struct(valueSchema)
                        .put("after", new Struct(valueAfterBeforeSchema)
                                .put("Id", "1")
                                .put("Name", "Name")
                                .put("timestamp", LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli()))
                        .put("op", CdcDbzSchemaProcessor.debeziumOperation.c.name()),
                1
        ))));
    }

    @Test
    void testPutFailWithInvalidKeySchema() {
        var processor = new CdcDbzSchemaProcessor();
        assertThrows(InvalidStructException.class, () -> processor.put(List.of(new SinkRecord(
                "test_topic",
                0,
                keySchema,
                "invalid_key", // This should be a Struct, but it's a String
                valueSchema,
                new Struct(valueSchema)
                        .put("after", new Struct(valueAfterBeforeSchema)
                                .put("Id", "1")
                                .put("Name", "Name")
                                .put("timestamp", LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli()))
                        .put("op", CdcDbzSchemaProcessor.debeziumOperation.c.name()),
                1
        ))));
    }

    @Test
    void testPutFailWithFieldOpMissing() {
        var valueSchemaWithNoOpField = SchemaBuilder.struct()
                .field("before", valueAfterBeforeSchema)
                .field("after", valueAfterBeforeSchema)
                .build();
        var processor = new CdcDbzSchemaProcessor();
        assertThrows(InvalidStructException.class, () -> processor.put(List.of(new SinkRecord(
                "test_topic",
                0,
                keySchema,
                new Struct(keySchema).put("id", "1"),
                valueSchemaWithNoOpField,
                new Struct(valueSchema)
                        .put("after", new Struct(valueAfterBeforeSchema)
                                .put("Id", "1")
                                .put("Name", "Name")
                                .put("timestamp", LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli()))
                        .put("op", CdcDbzSchemaProcessor.debeziumOperation.c.name()),
                1
        ))));
    }

    @Test
    void testPutFailWithValueOpMissing() {
        var processor = new CdcDbzSchemaProcessor();
        assertThrows(InvalidStructException.class, () -> processor.put(List.of(new SinkRecord(
                "test_topic",
                0,
                keySchema,
                new Struct(keySchema).put("id", "1"),
                valueSchema,
                new Struct(valueSchema)
                        .put("after", new Struct(valueAfterBeforeSchema)
                                .put("Id", "1")
                                .put("Name", "Name")
                                .put("timestamp", LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli())),
                1
        ))));
    }

    @Test
    void testPutFailWithNullKey() {
        var processor = new CdcDbzSchemaProcessor();
        assertThrows(InvalidStructException.class, () -> processor.put(List.of(new SinkRecord(
                "test_topic",
                0,
                keySchema,
                new Struct(keySchema).put("id", null), // Key should not be null
                valueSchema,
                new Struct(valueSchema)
                        .put("after", new Struct(valueAfterBeforeSchema)
                                .put("Id", "1")
                                .put("Name", "Name")
                                .put("timestamp", LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli()))
                        .put("op", CdcDbzSchemaProcessor.debeziumOperation.c.name()),
                1
        ))));
    }

    @Test
    void testFlushWithSuccess() throws SQLException, IOException {
        var processor = new CdcDbzSchemaProcessor();
        var dt = LocalDateTime.of(2018, 1, 10, 10, 30, 40);
        var statementMock = prepareToFlush(processor);
        processor.put(generateCreateEvents(dt, "1", "2", "3"));
        processor.put(generateUpdateEvents(dt, "new", "4"));
        processor.put(generateDeleteEvents(dt, "5"));
        processor.flush(null);

        verify(processor.snowflakeConnection, times(1)).uploadStream(any(), eq("/"),
                assertArg(c -> assertNotNull(c, "CSV data stream should should not be null")), any(), eq(true));
        verify(statementMock, times(1)).executeLargeUpdate(matches("COPY.*"));
        verify(statementMock, times(1)).executeLargeUpdate(matches("MERGE.*"));
        verify(statementMock, times(1)).executeLargeUpdate(matches("DELETE(.*)final.id = ingest.id"));
        assertEquals(0, processor.buffer.size(), "Buffer map should be empty after flush");
        assertTrue(Files.isDirectory(Path.of("/mnt/data/csv_data_to_stage/test_table")));
        assertTrue(Files.list(Path.of("/mnt/data/csv_data_to_stage/test_table")).toList().isEmpty());
    }

    @Test
    void testPrepareOrderedColumnsBasedOnTargetTableSuccess() throws Throwable {
        var processor = new CdcDbzSchemaProcessor();
        var dt = LocalDateTime.of(2018, 1, 10, 10, 30, 40);
        prepareToFlush(processor);
        processor.put(generateCreateEvents(dt, "1"));
        processor.put(generateDeleteEvents(dt, "2"));
        var blockID = "111";
        var tableBuffer = processor.buffer.get("test_table");
        var csvBaos = processor.prepareOrderedColumnsBasedOnTargetTable(blockID,
                List.of("id", "name", "timestamp", "time", "date", "desc", IHTOPIC, IHOFFSET, IHPARTITION, IHOP, IHBLOCKID, IHDATETIME),
                tableBuffer, "test_table");
        var pattern = Pattern.compile("""
                "1","Name 1","2018-01-10T08:30:40","10:30:40","2018-01-09",,"test_topic","0","0","c","111",(?<msgtimestampc>.*)
                "2","Name 2","2018-01-10T08:30:40","10:30:40","2018-01-09",,"test_topic","0","0","d","111",(?<msgtimestampd>.*)
                """);
        var fileData = IOUtils.toString(Files.newInputStream(csvBaos), "UTF-8");
        var files = Files.list(Path.of("/mnt/data/csv_data_to_stage/test_table")).toList();
        for (Path f : files) {
            Files.deleteIfExists(Path.of(f.toFile().getAbsolutePath()));
        }
        assertTrue(pattern.matcher(fileData).find(), String.format("CSV data [%s] should match with regex %s", fileData, pattern.pattern()));
    }

    @Test
    void testStartCleanUpJobSuccess() throws SchedulerException, SQLException {
        var processor = new CdcDbzSchemaProcessor();
        var dt = LocalDateTime.of(2018, 1, 10, 10, 30, 40);
        var statement = prepareToFlush(processor);
        // put() registers "test_table_INGEST" in knownIngestTables via getOrCreateBuffer
        processor.put(generateCreateEvents(dt, "1"));
        var props = generateConfig().originals();
        props.put(SnowflakeSinkConnector.CFG_JOB_CLEANUP_DURATION, "PT1S");
        processor.startCleanUpJob(new AbstractConfig(SnowflakeSinkConnector.CONFIG_DEF, props));
        verify(statement, timeout(4000).atLeast(3)).executeLargeUpdate(matches("delete.*"));
    }

    private Statement prepareToFlush(CdcDbzSchemaProcessor processor) throws SQLException {
        var statementMock = mockConnections(processor,
                List.of("id", "name", "timestamp", "time", "date", "desc"),
                List.of("id", "name", "timestamp", "time", "date", "desc", "ih_topic", "ih_offset", "ih_partition", "ih_op", "ih_datetime", "ih_blockid"),
                "test_table", "test_table_INGEST");
        processor.configParameters(generateConfig());
        return statementMock;
    }

    private AbstractConfig generateConfig() {
        return new AbstractConfig(SnowflakeSinkConnector.CONFIG_DEF, Map.of(
                "schema", "test_schema",
                "table", "test_table",
                "stage", "test_stage",
                "timestamp_fields_convert", "timestamp",
                "date_fields_convert", "date",
                "time_fields_convert", "time",
                "find_columns_in_metadata", Boolean.TRUE
        ));
    }

    private Statement mockConnections(CdcDbzSchemaProcessor processor, List<String> columnsTable, List<String> columnsTableIngest, String table, String tableIngest) throws SQLException {
        processor.snowflakeConnection = mock(SnowflakeConnection.class);
        processor.connection = mock(Connection.class);
        var dbMetadataMock = mock(DatabaseMetaData.class);
        var resultSetMock = mock(ResultSet.class);
        var resultSetTableMock = mock(ResultSet.class);
        var resultSetTableIngestMock = mock(ResultSet.class);
        var statementMock = mock(java.sql.Statement.class);

        when(processor.connection.createStatement()).thenReturn(statementMock);
        when(statementMock.executeQuery(any())).thenReturn(resultSetMock);
        when(processor.connection.getCatalog()).thenReturn("test_catalog");
        when(processor.connection.getSchema()).thenReturn("test_schema");
        when(processor.connection.getMetaData()).thenReturn(dbMetadataMock);

        var columnsTableIterator = columnsTable.iterator();
        when(resultSetTableMock.next()).thenAnswer(a -> {
            if (columnsTableIterator.hasNext()) {
                return true;
            }
            return false;
        });
        when(resultSetTableMock.getString("COLUMN_NAME")).thenAnswer(a -> {
            if (columnsTableIterator.hasNext()) {
                return columnsTableIterator.next();
            }
            return null;
        });


        var columnsTableIngestIterator = columnsTableIngest.iterator();
        when(resultSetTableIngestMock.next()).thenAnswer(a -> {
            if (columnsTableIngestIterator.hasNext()) {
                return true;
            }
            return false;
        });
        when(resultSetTableIngestMock.getString("COLUMN_NAME")).thenAnswer(a -> {
            if (columnsTableIngestIterator.hasNext()) {
                return columnsTableIngestIterator.next();
            }
            return null;
        });

        when(dbMetadataMock.getColumns(any(), any(), eq(table.toUpperCase()), any())).thenReturn(resultSetTableMock);
        when(dbMetadataMock.getColumns(any(), any(), eq(tableIngest.toUpperCase()), any())).thenReturn(resultSetTableIngestMock);

        when(processor.connection.unwrap(SnowflakeConnection.class)).thenReturn(processor.snowflakeConnection);
        when(processor.connection.getMetaData()).thenReturn(dbMetadataMock);
        return statementMock;
    }

    private Collection<SinkRecord> generateCreateEvents(LocalDateTime dt, String... ids) {
        return generateCreateEventsForTopic(dt, "test_topic", ids);
    }

    private Collection<SinkRecord> generateCreateEventsForTopic(LocalDateTime dt, String topic, String... ids) {
        var records = new ArrayList<SinkRecord>();

        for (int i = 0; i < ids.length; i++) {
            var id = ids[i];
            records.add(new SinkRecord(
                    topic,
                    0,
                    keySchema,
                    new Struct(keySchema).put("id", id),
                    valueSchema,
                    new Struct(valueSchema)
                            .put("after", new Struct(valueAfterBeforeSchema)
                                    .put("Id", id)
                                    .put("Name", "Name " + id)
                                    .put("timestamp", dt.toInstant(ZoneOffset.UTC).toEpochMilli())
                                    .put("time", dt.getLong(ChronoField.NANO_OF_DAY))
                                    .put("date", (int) dt.getLong(ChronoField.EPOCH_DAY)))
                            .put("op", CdcDbzSchemaProcessor.debeziumOperation.c.name()),
                    i
            ));
        }

        return records;
    }

    private Collection<SinkRecord> generateUpdateEvents(LocalDateTime dt, String nameSuffix, String... ids) {
        var records = new ArrayList<SinkRecord>();

        for (int i = 0; i < ids.length; i++) {
            var id = ids[i];
            records.add(new SinkRecord(
                    "test_topic",
                    0,
                    keySchema,
                    new Struct(keySchema).put("id", id),
                    valueSchema,
                    new Struct(valueSchema)
                            .put("before", new Struct(valueAfterBeforeSchema)
                                    .put("Id", id)
                                    .put("Name", "Name " + id)
                                    .put("timestamp", dt.toInstant(ZoneOffset.UTC).toEpochMilli())
                                    .put("time", dt.getLong(ChronoField.NANO_OF_DAY))
                                    .put("date", (int) dt.getLong(ChronoField.EPOCH_DAY)))
                            .put("after", new Struct(valueAfterBeforeSchema)
                                    .put("Id", id)
                                    .put("Name", String.format("Name %s %s", nameSuffix, id))
                                    .put("timestamp", dt.toInstant(ZoneOffset.UTC).toEpochMilli())
                                    .put("time", dt.getLong(ChronoField.NANO_OF_DAY))
                                    .put("date", (int) dt.getLong(ChronoField.EPOCH_DAY)))
                            .put("op", CdcDbzSchemaProcessor.debeziumOperation.u.name()),
                    i
            ));
        }

        return records;
    }

    private Collection<SinkRecord> generateDeleteEvents(LocalDateTime dt, String... ids) {
        var records = new ArrayList<SinkRecord>();

        for (int i = 0; i < ids.length; i++) {
            var id = ids[i];
            records.add(new SinkRecord(
                    "test_topic",
                    0,
                    keySchema,
                    new Struct(keySchema).put("id", id),
                    valueSchema,
                    new Struct(valueSchema)
                            .put("before", new Struct(valueAfterBeforeSchema)
                                    .put("Id", id)
                                    .put("Name", "Name " + id)
                                    .put("timestamp", dt.toInstant(ZoneOffset.UTC).toEpochMilli())
                                    .put("time", dt.getLong(ChronoField.NANO_OF_DAY))
                                    .put("date", (int) dt.getLong(ChronoField.EPOCH_DAY)))
                            .put("op", CdcDbzSchemaProcessor.debeziumOperation.d.name()),
                    i
            ));
        }

        return records;
    }
}
