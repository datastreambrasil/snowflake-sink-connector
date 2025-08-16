package br.com.datastreambrasil.v3;

import br.com.datastreambrasil.v3.exception.InvalidStructException;
import net.snowflake.client.jdbc.SnowflakeConnection;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.quartz.SchedulerException;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static br.com.datastreambrasil.v3.AbstractProcessor.IHBLOCKID;
import static br.com.datastreambrasil.v3.AbstractProcessor.IHDATETIME;
import static br.com.datastreambrasil.v3.AbstractProcessor.IHOFFSET;
import static br.com.datastreambrasil.v3.AbstractProcessor.IHOP;
import static br.com.datastreambrasil.v3.AbstractProcessor.IHPARTITION;
import static br.com.datastreambrasil.v3.AbstractProcessor.IHTOPIC;
import static br.com.datastreambrasil.v3.AbstractProcessor.IH_CURRENT_HASH;
import static br.com.datastreambrasil.v3.AbstractProcessor.IH_PREVIOUS_HASH;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
            .field("id", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
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
        var dt = LocalDateTime.of(2025, 1, 20, 10, 30, 40);
        processor.put(generateCreateEvents(dt, "1", "2", "3"));
        processor.put(generateUpdateEvents(dt, null, "update 001", "10"));//this update should be ignored, because it will be overridden by the next update event
        processor.put(generateUpdateEvents(dt, null, "update 002", "10"));
        processor.put(generateDeleteEvents(dt, "20"));
        processor.put(generateDeleteEvents(dt, "3")); //this delete will override the create event for id 3
        assertEquals(5, processor.buffer.size());

        var itemIdx = "1";
        //assert item create
        assertEquals(itemIdx, processor.buffer.get(itemIdx).event().get("id"));
        assertEquals("Name " + itemIdx, processor.buffer.get(itemIdx).event().get("name"));
        assertEquals("c", processor.buffer.get(itemIdx).op());

        //assert item update
        itemIdx = "10";
        assertEquals(itemIdx, processor.buffer.get(itemIdx).event().get("id"));
        assertEquals("Name update 002 " + itemIdx, processor.buffer.get(itemIdx).event().get("name"));
        assertEquals("u", processor.buffer.get(itemIdx).op());

        //asert item delete
        itemIdx = "20";
        assertEquals(itemIdx, processor.buffer.get(itemIdx).event().get("id"));
        assertEquals("Name " + itemIdx, processor.buffer.get(itemIdx).event().get("name"));
        assertEquals("d", processor.buffer.get(itemIdx).op());

        itemIdx = "3";
        assertEquals(itemIdx, processor.buffer.get(itemIdx).event().get("id"));
        assertEquals("Name " + itemIdx, processor.buffer.get(itemIdx).event().get("name"));
        assertEquals("d", processor.buffer.get(itemIdx).op());
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
                    .put("id", "1")
                    .put("name", "Name")
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
                    .put("id", "1")
                    .put("name", "Name")
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
                    .put("id", "1")
                    .put("name", "Name")
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
                    .put("id", "1")
                    .put("name", "Name")
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
                    .put("id", "1")
                    .put("name", "Name")
                    .put("timestamp", LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli()))
                .put("op", CdcDbzSchemaProcessor.debeziumOperation.c.name()),
            1
        ))));
    }

    @Test
    void testFlushWithSuccess() throws SQLException {
        var processor = new CdcDbzSchemaProcessor();
        var dt = LocalDateTime.of(2018, 1, 10, 10, 30, 40);
        var statementMock = prepareToFlush(processor, null, null, null);
        processor.put(generateCreateEvents(dt, "1", "2", "3"));
        processor.put(generateUpdateEvents(dt, null, "new", "4"));
        processor.put(generateDeleteEvents(dt, "5"));
        processor.flush(null);

        verify(processor.snowflakeConnection, times(1)).uploadStream(any(), eq("/"), assertArg(c -> assertEquals(769, c.available(), "CSV data length should be 459 bytes")), any(), eq(true));
        verify(statementMock, times(1)).executeUpdate(matches("MERGE.*"));
        verify(statementMock, times(1)).executeUpdate(matches("DELETE(.*)final.id = ingest.id"));
        verify(statementMock, times(1)).executeUpdate(matches("COPY.*"));
        assertEquals(0, processor.buffer.size(), "Buffer should be empty after flush");
    }

    @Test
    void testCsvFormatSuccess() throws Throwable {
        var processor = new CdcDbzSchemaProcessor();
        var dt = LocalDateTime.of(2018, 1, 10, 10, 30, 40);
        prepareToFlush(processor, null, null, null);
        processor.put(generateCreateEvents(dt, "1"));
        processor.put(generateDeleteEvents(dt, "2"));
        var blockID = "111";
        var csvBaos = processor.prepareOrderedColumnsBasedOnTargetTable(blockID, List.of("id", "name", "timestamp", "time", "date", "desc", IHTOPIC, IHOFFSET, IHPARTITION, IHOP, IHBLOCKID, IHDATETIME));
        var pattern = Pattern.compile("""
            "1","Name 1","2018-01-10T08:30:40","10:30:40","2018-01-09",,"test_topic","0","0","c","111",(?<msgtimestampc>.*)
            "2","Name 2","2018-01-10T08:30:40","10:30:40","2018-01-09",,"test_topic","0","0","d","111",(?<msgtimestampd>.*)
            """);

        assertTrue(pattern.matcher(csvBaos.toString()).find(), String.format("CSV data [%s] should match with regex %s", csvBaos, pattern.pattern()));
    }

    @Test
    void testCsvFormatHashingCreateSuccess() throws Throwable {
        var processor = new CdcDbzSchemaProcessor();
        var dt = LocalDateTime.of(2018, 1, 10, 10, 30, 40);
        prepareToFlush(processor, Map.of(SnowflakeSinkConnector.CFG_HASHING_SUPPORT, true), List.of(IH_CURRENT_HASH, IH_PREVIOUS_HASH),
            List.of(IH_CURRENT_HASH, IH_PREVIOUS_HASH));
        processor.put(generateCreateEvents(dt, "1")); //hash CR32 --> 654df8a3
        var blockID = "111";
        var csvBaos = processor.prepareOrderedColumnsBasedOnTargetTable(blockID, List.of("id", "name", "timestamp", "time", "date", "desc", IHTOPIC, IHOFFSET, IHPARTITION, IHOP, IHBLOCKID, IH_CURRENT_HASH, IH_PREVIOUS_HASH, IHDATETIME));
        var pattern = Pattern.compile("""
            "1","Name 1","2018-01-10T08:30:40","10:30:40","2018-01-09",,"test_topic","0","0","c","111","654df8a3",,(?<msgtimestampc>.*)
            """);

        assertTrue(pattern.matcher(csvBaos.toString()).find(), String.format("CSV data [%s] should match with regex %s", csvBaos, pattern.pattern()));
    }

    @Test
    void testCsvFormatUpdateSuccess() throws Throwable {
        var processor = new CdcDbzSchemaProcessor();
        var dt = LocalDateTime.of(2018, 1, 10, 10, 30, 40);
        prepareToFlush(processor, Map.of(SnowflakeSinkConnector.CFG_HASHING_SUPPORT, true), List.of(IH_CURRENT_HASH, IH_PREVIOUS_HASH),
            List.of(IH_CURRENT_HASH, IH_PREVIOUS_HASH));
        processor.put(generateUpdateEvents(dt, null, "new", "1")); //hash CR32 --> 388383f0
        var blockID = "111";
        var csvBaos = processor.prepareOrderedColumnsBasedOnTargetTable(blockID, List.of("id", "name", "timestamp", "time", "date", "desc", IHTOPIC, IHOFFSET, IHPARTITION, IHOP, IHBLOCKID, IH_CURRENT_HASH, IH_PREVIOUS_HASH, IHDATETIME));
        var pattern = Pattern.compile("""
            "1","Name new 1","2018-01-10T08:30:40","10:30:40","2018-01-09",,"test_topic","0","0","u","111","388383f0","654df8a3",(?<msgtimestampc>.*)
            """);

        assertTrue(pattern.matcher(csvBaos.toString()).find(), String.format("CSV data [%s] should match with regex %s", csvBaos, pattern.pattern()));
    }

    @Test
    void testCsvFormatDeleteSuccess() throws Throwable {
        var processor = new CdcDbzSchemaProcessor();
        var dt = LocalDateTime.of(2018, 1, 10, 10, 30, 40);
        prepareToFlush(processor, Map.of(SnowflakeSinkConnector.CFG_HASHING_SUPPORT, true), List.of(IH_CURRENT_HASH, IH_PREVIOUS_HASH),
            List.of(IH_CURRENT_HASH, IH_PREVIOUS_HASH));
        processor.put(generateCreateEvents(dt, "1")); //hash CR32 --> 654df8a3
        processor.put(generateDeleteEvents(dt, "1")); //hash CR32 --> 654df8a3
        var blockID = "111";
        var csvBaos = processor.prepareOrderedColumnsBasedOnTargetTable(blockID, List.of("id", "name", "timestamp", "time", "date", "desc", IHTOPIC, IHOFFSET, IHPARTITION, IHOP, IHBLOCKID, IH_CURRENT_HASH, IH_PREVIOUS_HASH, IHDATETIME));
        var pattern = Pattern.compile("""
            "1","Name 1","2018-01-10T08:30:40","10:30:40","2018-01-09",,"test_topic","0","0","d","111",,"654df8a3",(?<msgtimestampd>.*)
            """);

        assertTrue(pattern.matcher(csvBaos.toString()).find(), String.format("CSV data [%s] should match with regex %s", csvBaos, pattern.pattern()));
    }

    @Test
    void testCsvFormatDuplicatedEventsSuccess() throws Throwable {
        var processor = new CdcDbzSchemaProcessor();
        var dt = LocalDateTime.of(2018, 1, 10, 10, 30, 40);
        prepareToFlush(processor, Map.of(SnowflakeSinkConnector.CFG_HASHING_SUPPORT, true), List.of(IH_CURRENT_HASH, IH_PREVIOUS_HASH),
            List.of(IH_CURRENT_HASH, IH_PREVIOUS_HASH));
        processor.put(generateCreateEvents(dt, "1")); //hash CR32 --> 654df8a3
        processor.put(generateCreateEvents(dt, "1")); //hash CR32 --> 654df8a3
        processor.put(generateDeleteEvents(dt, "2")); //hash CR32 --> e8cc0cca
        processor.put(generateDeleteEvents(dt, "2")); //hash CR32 --> e8cc0cca
        processor.put(generateUpdateEvents(dt, null, "new", "3")); //hash CR32 --> 3d3f9965
        processor.put(generateUpdateEvents(dt, null, "new", "3")); //hash CR32 --> 3d3f9965
        var blockID = "111";
        var csvBaos = processor.prepareOrderedColumnsBasedOnTargetTable(blockID, List.of("id", "name", "timestamp", "time", "date", "desc", IHTOPIC, IHOFFSET, IHPARTITION, IHOP, IHBLOCKID, IH_CURRENT_HASH, IH_PREVIOUS_HASH, IHDATETIME));
        var pattern = Pattern.compile("""
            "1","Name 1","2018-01-10T08:30:40","10:30:40","2018-01-09",,"test_topic","0","0","c","111","654df8a3",,(?<msgtimestampc>.*)
            "2","Name 2","2018-01-10T08:30:40","10:30:40","2018-01-09",,"test_topic","0","0","d","111",,"e8cc0cca",(?<msgtimestampd>.*)
            "3","Name new 3","2018-01-10T08:30:40","10:30:40","2018-01-09",,"test_topic","0","0","u","111","3d3f9965","2563a2d2",(?<msgtimestampu>.*)
            """);

        assertTrue(pattern.matcher(csvBaos.toString()).find(), String.format("CSV data [%s] should match with regex %s", csvBaos, pattern.pattern()));
    }

    @Test
    void testCsvFormatManageSameObjectCreateSuccess() throws Throwable {
        var processor = new CdcDbzSchemaProcessor();
        var dt = LocalDateTime.of(2018, 1, 10, 10, 30, 40);
        prepareToFlush(processor, Map.of(SnowflakeSinkConnector.CFG_HASHING_SUPPORT, true), List.of(IH_CURRENT_HASH, IH_PREVIOUS_HASH),
            List.of(IH_CURRENT_HASH, IH_PREVIOUS_HASH));
        processor.put(generateCreateEvents(dt, "1")); //hash CR32 --> 654df8a3
        processor.put(generateUpdateEvents(dt, null, "new", "1")); //hash CR32 --> 388383f0
        var blockID = "111";
        var csvBaos = processor.prepareOrderedColumnsBasedOnTargetTable(blockID, List.of("id", "name", "timestamp", "time", "date", "desc", IHTOPIC, IHOFFSET, IHPARTITION, IHOP, IHBLOCKID, IH_CURRENT_HASH, IH_PREVIOUS_HASH, IHDATETIME));
        var pattern = Pattern.compile("""
            "1","Name new 1","2018-01-10T08:30:40","10:30:40","2018-01-09",,"test_topic","0","0","u","111","388383f0","654df8a3",(?<msgtimestampc>.*)
            """);

        assertTrue(pattern.matcher(csvBaos.toString()).find(), String.format("CSV data [%s] should match with regex %s", csvBaos, pattern.pattern()));
    }

    @Test
    void testCsvFormatManageSameObjectManyUpdatesSuccess() throws Throwable {
        var processor = new CdcDbzSchemaProcessor();
        var dt = LocalDateTime.of(2018, 1, 10, 10, 30, 40);
        prepareToFlush(processor, Map.of(SnowflakeSinkConnector.CFG_HASHING_SUPPORT, true), List.of(IH_CURRENT_HASH, IH_PREVIOUS_HASH),
            List.of(IH_CURRENT_HASH, IH_PREVIOUS_HASH));
        processor.put(generateCreateEvents(dt, "1")); //hash CR32 --> 654df8a3
        processor.put(generateUpdateEvents(dt, null, "new", "1")); //hash CR32 --> 388383f0
        processor.put(generateUpdateEvents(dt, "new", "new 2", "1")); //hash CR32 --> 2ec1cb58
        var blockID = "111";
        var csvBaos = processor.prepareOrderedColumnsBasedOnTargetTable(blockID, List.of("id", "name", "timestamp", "time", "date", "desc", IHTOPIC, IHOFFSET, IHPARTITION, IHOP, IHBLOCKID, IH_CURRENT_HASH, IH_PREVIOUS_HASH, IHDATETIME));
        var pattern = Pattern.compile("""
            "1","Name new 2 1","2018-01-10T08:30:40","10:30:40","2018-01-09",,"test_topic","0","0","u","111","2ec1cb58","xx",(?<msgtimestampc>.*)
            """);

        assertTrue(pattern.matcher(csvBaos.toString()).find(), String.format("CSV data [%s] should match with regex %s", csvBaos, pattern.pattern()));
    }

    @Test
    void testCsvFormatManyUpdatesHashesSameObjectSuccess() throws Throwable {
        var processor = new CdcDbzSchemaProcessor();
        var dt = LocalDateTime.of(2018, 1, 10, 10, 30, 40);
        prepareToFlush(processor, Map.of(SnowflakeSinkConnector.CFG_HASHING_SUPPORT, true), List.of(IH_CURRENT_HASH, IH_PREVIOUS_HASH),
            List.of(IH_CURRENT_HASH, IH_PREVIOUS_HASH));
        processor.put(generateUpdateEvents(dt, null, "new", "1")); //hash CR32 --> 388383f0
        processor.put(generateUpdateEvents(dt, "new", "new 002", "1")); //hash CR32 --> d0a1681c
        processor.put(generateUpdateEvents(dt, "new 002", "new 003", "1")); //hash CR32 --> b9ed6ded
        var blockID = "111";
        var csvBaos = processor.prepareOrderedColumnsBasedOnTargetTable(blockID, List.of("id", "name", "timestamp", "time", "date", "desc", IHTOPIC, IHOFFSET, IHPARTITION, IHOP, IHBLOCKID, IH_CURRENT_HASH, IH_PREVIOUS_HASH, IHDATETIME));
        var pattern = Pattern.compile("""
            "1","Name new 003 1","2018-01-10T08:30:40","10:30:40","2018-01-09",,"test_topic","0","0","u","111","b9ed6ded","388383f0",(?<msgtimestampc>.*)
            """);

        assertTrue(pattern.matcher(csvBaos.toString()).find(), String.format("CSV data [%s] should match with regex %s", csvBaos, pattern.pattern()));
    }

    @Test
    void testStartCleanUpJobSuccess() throws SchedulerException, SQLException {
        var processor = new CdcDbzSchemaProcessor();
        var statement = prepareToFlush(processor, null, null, null);
        var props = generateConfig(null).originals();
        props.put(SnowflakeSinkConnector.CFG_JOB_CLEANUP_DURATION, "PT1S");
        processor.startCleanUpJob(new AbstractConfig(SnowflakeSinkConnector.CONFIG_DEF, props));
        verify(statement, timeout(4000).atLeast(3)).executeUpdate(matches("delete.*"));
    }

    private Statement prepareToFlush(CdcDbzSchemaProcessor processor, Map<String, Object> extraProps,
                                     List<String> extraFinalFields, List<String> extraIngestTableFields) throws SQLException {
        if (extraFinalFields == null) {
            extraFinalFields = new ArrayList<>();
        }
        if (extraIngestTableFields == null) {
            extraIngestTableFields = new ArrayList<>();
        }
        var ingestTableColumns = new ArrayList<>(List.of("id", "name", "timestamp", "time", "date", "desc", "ih_topic", "ih_offset", "ih_partition", "ih_op", "ih_datetime", "ih_blockid"));
        var finalTableColumns = new ArrayList<>(List.of("id", "name", "timestamp", "time", "date", "desc"));
        finalTableColumns.addAll(extraFinalFields);
        ingestTableColumns.addAll(extraIngestTableFields);
        var statementMock = mockConnections(processor, finalTableColumns, ingestTableColumns, "test_table", "test_table_INGEST");
        processor.configParameters(generateConfig(extraProps));
        processor.configMetadata();
        return statementMock;
    }

    private AbstractConfig generateConfig(Map<String, Object> extraProps) {
        if (extraProps == null) {
            extraProps = new HashMap<>();
        }
        Map<String, Object> props = new HashMap<>(Map.of(
            "schema", "test_schema",
            "table", "test_table",
            "stage", "test_stage",
            "timestamp_fields_convert", "timestamp",
            "date_fields_convert", "date",
            "time_fields_convert", "time"
        ));
        props.putAll(extraProps);

        return new AbstractConfig(SnowflakeSinkConnector.CONFIG_DEF, props);
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
                    .put("after", new Struct(valueAfterBeforeSchema)
                        .put("id", id)
                        .put("name", "Name " + id)
                        .put("timestamp", dt.toInstant(ZoneOffset.UTC).toEpochMilli())
                        .put("time", dt.getLong(ChronoField.NANO_OF_DAY))
                        .put("date", (int) dt.getLong(ChronoField.EPOCH_DAY)))
                    .put("op", CdcDbzSchemaProcessor.debeziumOperation.c.name()),
                i
            ));
        }

        return records;
    }

    private Collection<SinkRecord> generateUpdateEvents(LocalDateTime dt, String previousNameSuffix, String nameSuffix, String... ids) {
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
                        .put("id", id)
                        .put("name", previousNameSuffix == null ? String.format("Name %s", id) : String.format("Name %s %s", previousNameSuffix, id))
                        .put("timestamp", dt.toInstant(ZoneOffset.UTC).toEpochMilli())
                        .put("time", dt.getLong(ChronoField.NANO_OF_DAY))
                        .put("date", (int) dt.getLong(ChronoField.EPOCH_DAY)))
                    .put("after", new Struct(valueAfterBeforeSchema)
                        .put("id", id)
                        .put("name", String.format("Name %s %s", nameSuffix, id))
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
                        .put("id", id)
                        .put("name", "Name " + id)
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