package br.com.datastreambrasil.v3;

import net.snowflake.client.jdbc.SnowflakeConnection;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CdcDbzSchemaProcessorMultiTableTest {

    private static Schema valueAfterBeforeSchema;
    private static Schema valueSchema;
    private static Schema keySchema;

    @BeforeAll
    static void beforeTest() {
        valueAfterBeforeSchema = SchemaBuilder.struct()
                .field("Id", Schema.STRING_SCHEMA)
                .field("Name", Schema.STRING_SCHEMA)
                .build();
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
    void testFlushWithSuccess_ThreeTables_noException() throws SQLException {
        var processor = new CdcDbzSchemaProcessor();
        processor.processMultiTables = true;
        var statementMock = prepareForMultiTableFlush(processor, List.of("tabelaX", "tabelaY", "tabelaZ"));
        var dt = LocalDateTime.of(2025, 6, 3, 10, 0, 0);

        processor.put(generateCreateEventsForTopic(dt, "schema.topicA.tabelaX", "1", "2"));
        processor.put(generateCreateEventsForTopic(dt, "schema.topicB.tabelaY", "3"));
        processor.put(generateCreateEventsForTopic(dt, "schema.topicC.tabelaZ", "4", "5"));

        assertEquals(3, processor.buffer.size(), "Buffer must have 3 tables before flush");

        assertDoesNotThrow(() -> processor.flush(null),
                "flush() must not throw ConcurrentModificationException when iterating over multiple tables");
    }

    @Test
    void testFlushWithSuccess_TwoTables_eachTableGetsCopyAndMergeStatements() throws SQLException {
        var processor = new CdcDbzSchemaProcessor();
        processor.processMultiTables = true;
        var statementMock = prepareForMultiTableFlush(processor, List.of("tabelaX", "tabelaZ"));
        var dt = LocalDateTime.of(2025, 6, 3, 10, 0, 0);

        processor.put(generateCreateEventsForTopic(dt, "schema.topicA.tabelaX", "1"));
        processor.put(generateCreateEventsForTopic(dt, "schema.topicB.tabelaZ", "2"));

        processor.flush(null);

        verify(statementMock, times(2)).executeLargeUpdate(matches("COPY.*"));
        verify(statementMock, times(2)).executeLargeUpdate(matches("MERGE.*"));
    }

    @Test
    void testFlushWithSuccess_ThreeTables_bufferEmptyAfterFlush() throws SQLException {
        var processor = new CdcDbzSchemaProcessor();
        processor.processMultiTables = true;
        prepareForMultiTableFlush(processor, List.of("tabelaX", "tabelaY", "tabelaZ"));
        var dt = LocalDateTime.of(2025, 6, 3, 10, 0, 0);

        processor.put(generateCreateEventsForTopic(dt, "schema.topicA.tabelaX", "1"));
        processor.put(generateCreateEventsForTopic(dt, "schema.topicB.tabelaY", "2"));
        processor.put(generateCreateEventsForTopic(dt, "schema.topicC.tabelaZ", "3"));

        processor.flush(null);

        assertEquals(0, processor.buffer.size(), "All table entries must be removed from buffer after flush");
    }

    @Test
    void testFlushWithSuccess_MultiTable_stageNameIsTableBaseName() throws SQLException {
        var processor = new CdcDbzSchemaProcessor();
        processor.processMultiTables = true;
        prepareForMultiTableFlush(processor, List.of("tabelaX"));
        var dt = LocalDateTime.of(2025, 6, 3, 10, 0, 0);

        processor.put(generateCreateEventsForTopic(dt, "schema.topicA.tabelaX", "1"));
        processor.flush(null);

        verify(processor.snowflakeConnection, times(1))
                .uploadStream(eq("tabelaX"), eq("/"), any(), any(), eq(true));
    }

    @Test
    void testPutIsolation_recordsRoutedToCorrectBuffer() {
        var processor = new CdcDbzSchemaProcessor();
        processor.tableName = "default_table";
        processor.bufferInitialCapacity = 10;
        processor.processMultiTables = true;
        var dt = LocalDateTime.of(2025, 6, 3, 10, 0, 0);

        processor.put(generateCreateEventsForTopic(dt, "schema.src.tabelaX", "1", "2", "3"));
        processor.put(generateCreateEventsForTopic(dt, "schema.src.tabelaZ", "10", "20"));

        var bufferX = processor.buffer.get("tabelaX");
        var bufferZ = processor.buffer.get("tabelaZ");

        assertNotNull(bufferX, "Buffer for tabelaX must exist");
        assertNotNull(bufferZ, "Buffer for tabelaZ must exist");
        assertEquals(3, bufferX.size(), "tabelaX buffer must contain exactly 3 records");
        assertEquals(2, bufferZ.size(), "tabelaZ buffer must contain exactly 2 records");
        assertNull(bufferZ.get("1"), "Record '1' from tabelaX must not appear in tabelaZ buffer");
        assertNull(bufferX.get("10"), "Record '10' from tabelaZ must not appear in tabelaX buffer");
    }

    @Test
    void testPutIsolation_samePkInDifferentTables_doNotCollide() {
        var processor = new CdcDbzSchemaProcessor();
        processor.tableName = "default_table";
        processor.bufferInitialCapacity = 10;
        processor.processMultiTables = true;
        var dt = LocalDateTime.of(2025, 6, 3, 10, 0, 0);

        processor.put(generateCreateEventsForTopic(dt, "schema.src.tabelaX", "1"));
        processor.put(generateDeleteEventsForTopic(dt, "schema.src.tabelaZ", "1"));

        var bufferX = processor.buffer.get("tabelaX");
        var bufferZ = processor.buffer.get("tabelaZ");

        assertNotNull(bufferX.get("1+c"), "PK '1' (create) must exist in tabelaX buffer");
        assertNotNull(bufferZ.get("1+d"), "PK '1' (delete) must exist in tabelaZ buffer");
        assertEquals("c", bufferX.get("1+c").op(), "tabelaX pk='1' must be a create");
        assertEquals("d", bufferZ.get("1+d").op(), "tabelaZ pk='1' must be a delete");
    }

    @Test
    void testFlushWithSuccess_MixedOps_createInOneTable_deleteInOther() throws SQLException {
        var processor = new CdcDbzSchemaProcessor();
        processor.processMultiTables = true;
        var statementMock = prepareForMultiTableFlush(processor, List.of("tabelaX", "tabelaZ"));
        var dt = LocalDateTime.of(2025, 6, 3, 10, 0, 0);

        processor.put(generateCreateEventsForTopic(dt, "schema.topicA.tabelaX", "1"));
        processor.put(generateDeleteEventsForTopic(dt, "schema.topicB.tabelaZ", "2"));

        processor.flush(null);

        // Both tables trigger their own COPY
        verify(statementMock, times(2)).executeLargeUpdate(matches("COPY.*"));
        // Only tabelaX has inserts → 1 MERGE
        verify(statementMock, times(1)).executeLargeUpdate(matches("MERGE.*"));
        // Only tabelaZ has deletes → 1 DELETE
        verify(statementMock, times(1)).executeLargeUpdate(matches("DELETE.*"));
        assertEquals(0, processor.buffer.size(), "Buffer must be empty after flush");
    }

    @Test
    void testPutIsolation_multipleTopicsSameSuffix_distinctBufferEntries() {
        var processor = new CdcDbzSchemaProcessor();
        processor.tableName = "default_table";
        processor.bufferInitialCapacity = 10;
        processor.processMultiTables = true;
        var dt = LocalDateTime.of(2025, 6, 3, 10, 0, 0);

        // Different full topics but all resolve to the same table name via last segment
        processor.put(generateCreateEventsForTopic(dt, "prefix.source_a.orders", "1", "2"));
        processor.put(generateUpdateEventsForTopic(dt, "prefix.source_b.orders", "1"));

        // Both topics resolve to "orders" — create and update for id=1 coexist as separate entries (key = pk+op)
        var bufferOrders = processor.buffer.get("orders");
        assertNotNull(bufferOrders, "Buffer for 'orders' must exist");
        // 3 entries: 1+c (create), 2+c (create), 1+u (update)
        assertEquals(3, bufferOrders.size(), "Buffer must have 3 entries: create and update for id=1, and create for id=2");
        assertEquals("c", bufferOrders.get("1+c").op(), "id=1 create entry must exist");
        assertEquals("u", bufferOrders.get("1+u").op(), "id=1 update entry must exist");
        assertEquals("c", bufferOrders.get("2+c").op(), "id=2 must remain a create operation");
    }

    private Statement prepareForMultiTableFlush(CdcDbzSchemaProcessor processor,
                                                List<String> tableNames) throws SQLException {
        processor.snowflakeConnection = mock(SnowflakeConnection.class);
        processor.connection = mock(Connection.class);
        var statementMock = mock(Statement.class);
        when(processor.connection.createStatement()).thenReturn(statementMock);

        processor.configParameters(generateConfigMultiTables());

        for (String tableName : tableNames) {
            var upper = tableName.toUpperCase();
            processor.columnsIngestTable.put(upper + "_INGEST",
                    List.of("id", "name", "ih_topic", "ih_offset", "ih_partition", "ih_op", "ih_datetime", "ih_blockid"));
            processor.columnsFinalTable.put(upper, List.of("id", "name"));
        }

        return statementMock;
    }

    private AbstractConfig generateConfigMultiTables() {
        return new AbstractConfig(SnowflakeSinkConnector.CONFIG_DEF, Map.of(
                "schema", "test_schema",
                "table", "default_table",
                "stage", "test_stage",
                "find_columns_in_metadata", Boolean.FALSE,
                "process_multiples_tables", Boolean.TRUE
        ));
    }

    private Collection<SinkRecord> generateCreateEventsForTopic(LocalDateTime dt, String topic, String... ids) {
        var records = new ArrayList<SinkRecord>();
        for (int i = 0; i < ids.length; i++) {
            var id = ids[i];
            records.add(new SinkRecord(
                    topic, 0, keySchema,
                    new Struct(keySchema).put("id", id),
                    valueSchema,
                    new Struct(valueSchema)
                            .put("after", new Struct(valueAfterBeforeSchema)
                                    .put("Id", id)
                                    .put("Name", "Name " + id))
                            .put("op", CdcDbzSchemaProcessor.debeziumOperation.c.name()),
                    i));
        }
        return records;
    }

    private Collection<SinkRecord> generateUpdateEventsForTopic(LocalDateTime dt, String topic, String... ids) {
        var records = new ArrayList<SinkRecord>();
        for (int i = 0; i < ids.length; i++) {
            var id = ids[i];
            records.add(new SinkRecord(
                    topic, 0, keySchema,
                    new Struct(keySchema).put("id", id),
                    valueSchema,
                    new Struct(valueSchema)
                            .put("before", new Struct(valueAfterBeforeSchema)
                                    .put("Id", id)
                                    .put("Name", "Name " + id))
                            .put("after", new Struct(valueAfterBeforeSchema)
                                    .put("Id", id)
                                    .put("Name", "Name updated " + id))
                            .put("op", CdcDbzSchemaProcessor.debeziumOperation.u.name()),
                    i));
        }
        return records;
    }

    private Collection<SinkRecord> generateDeleteEventsForTopic(LocalDateTime dt, String topic, String... ids) {
        var records = new ArrayList<SinkRecord>();
        for (int i = 0; i < ids.length; i++) {
            var id = ids[i];
            records.add(new SinkRecord(
                    topic, 0, keySchema,
                    new Struct(keySchema).put("id", id),
                    valueSchema,
                    new Struct(valueSchema)
                            .put("before", new Struct(valueAfterBeforeSchema)
                                    .put("Id", id)
                                    .put("Name", "Name " + id))
                            .put("op", CdcDbzSchemaProcessor.debeziumOperation.d.name()),
                    i));
        }
        return records;
    }
}
