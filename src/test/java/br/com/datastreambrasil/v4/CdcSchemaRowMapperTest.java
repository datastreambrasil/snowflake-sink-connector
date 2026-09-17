package br.com.datastreambrasil.v4;

import br.com.datastreambrasil.v4.exception.InvalidStructException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CdcSchemaRowMapperTest {

    /** Column names as Snowflake reports them for unquoted identifiers. */
    private static final List<String> INGEST_COLUMNS = List.of(
            "ID", "NAME", "TS", "DT", "TM",
            "IH_TOPIC", "IH_PARTITION", "IH_OFFSET", "IH_OP", "IH_DATETIME", "IH_BLOCKID");

    private static Schema payloadSchema;
    private static Schema valueSchema;
    private static Schema keySchema;

    @BeforeAll
    static void beforeAll() {
        payloadSchema = SchemaBuilder.struct()
                .field("Id", Schema.STRING_SCHEMA)
                .field("Name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ts", Schema.OPTIONAL_INT64_SCHEMA)
                .field("dt", Schema.OPTIONAL_INT32_SCHEMA)
                .field("tm", Schema.OPTIONAL_INT64_SCHEMA)
                .build();
        valueSchema = SchemaBuilder.struct()
                .field("before", payloadSchema)
                .field("after", payloadSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();
        keySchema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .build();
    }

    private CdcSchemaRowMapper mapper() {
        return new CdcSchemaRowMapper(INGEST_COLUMNS, List.of(), List.of("TS"), List.of("DT"), List.of("TM"));
    }

    private SinkRecord record(String op, String id, String name, Long ts, Integer dt, Long tm, long offset) {
        var payload = new Struct(payloadSchema)
                .put("Id", id)
                .put("Name", name)
                .put("ts", ts)
                .put("dt", dt)
                .put("tm", tm);
        var value = new Struct(valueSchema).put("op", op);
        if ("d".equals(op)) {
            value.put("before", payload);
        } else {
            value.put("after", payload);
        }
        var key = new Struct(keySchema).put("id", id);
        return new SinkRecord("cdc.test.dbo.events", 3, keySchema, key, valueSchema, value, offset);
    }

    @Test
    void keysAreTheIngestColumnNamesAndMetadataIsStamped() {
        var row = mapper().toRow(record("c", "1", "Name 1", null, null, null, 42L), "block-1");

        assertEquals("1", row.get("ID"));
        assertEquals("Name 1", row.get("NAME"));
        assertEquals("cdc.test.dbo.events", row.get("IH_TOPIC"));
        assertEquals(3, row.get("IH_PARTITION"));
        assertEquals(42L, row.get("IH_OFFSET"));
        assertEquals("c", row.get("IH_OP"));
        assertEquals("block-1", row.get("IH_BLOCKID"));
        assertTrue(row.containsKey("IH_DATETIME"));

        // the pipe matches by column name, so every key must be a real ingest column
        assertTrue(INGEST_COLUMNS.containsAll(row.keySet()),
                "unexpected columns emitted: " + row.keySet());
    }

    @Test
    void deleteEventReadsTheBeforeStruct() {
        var row = mapper().toRow(record("d", "7", "gone", null, null, null, 9L), "block-1");

        assertEquals("d", row.get("IH_OP"));
        assertEquals("7", row.get("ID"));
        assertEquals("gone", row.get("NAME"));
    }

    @Test
    void nullFieldsAreOmittedSoTheyLandAsNull() {
        var row = mapper().toRow(record("c", "1", null, null, null, null, 0L), "block-1");

        assertFalse(row.containsKey("NAME"));
        assertFalse(row.containsKey("TS"));
        assertFalse(row.containsKey("DT"));
        assertFalse(row.containsKey("TM"));
    }

    @Test
    void convertsTimestampDateAndTimeColumnsLikeV3() {
        var instant = LocalDateTime.of(2025, 1, 20, 10, 30, 40)
                .atZone(TimeZone.getDefault().toZoneId()).toInstant();
        var epochDays = (int) LocalDate.of(2025, 1, 20).toEpochDay();
        var nanosOfDay = LocalTime.of(10, 30, 40).toNanoOfDay();

        var row = mapper().toRow(
                record("u", "1", "n", instant.toEpochMilli(), epochDays, nanosOfDay, 1L), "block-1");

        assertEquals(LocalDateTime.ofInstant(Instant.ofEpochMilli(instant.toEpochMilli()),
                TimeZone.getDefault().toZoneId()).toString(), row.get("TS"));
        assertEquals(LocalDate.ofInstant(Instant.ofEpochSecond(epochDays * 86400L),
                TimeZone.getDefault().toZoneId()).toString(), row.get("DT"));
        assertEquals("10:30:40", row.get("TM"));
    }

    @Test
    void columnsAbsentFromTheIngestTableAreNeverEmitted() {
        // NAME is not part of the ingest table here, so it must not reach the pipe
        var narrow = new CdcSchemaRowMapper(List.of("ID", "IH_OP"), List.of(), List.of(), List.of(), List.of());
        var row = narrow.toRow(record("c", "1", "Name 1", null, null, null, 0L), "block-1");

        assertEquals(2, row.size());
        assertEquals("1", row.get("ID"));
        assertEquals("c", row.get("IH_OP"));
    }

    @Test
    void extractsPrimaryKeyFromTheKeySchema() {
        assertEquals(List.of("id"), mapper().extractPk(record("c", "1", "n", null, null, null, 0L)));
    }

    @Test
    void rejectsRecordsWithoutStructKeyAndValue() {
        var invalid = new SinkRecord("t", 0, null, "key", null, "value", 0L);
        assertThrows(InvalidStructException.class, () -> mapper().toRow(invalid, "block-1"));
    }

    @Test
    void rejectsRecordsWithoutAnOperation() {
        var noOpSchema = SchemaBuilder.struct().field("after", payloadSchema).build();
        var value = new Struct(noOpSchema)
                .put("after", new Struct(payloadSchema).put("Id", "1"));
        var key = new Struct(keySchema).put("id", "1");
        var record = new SinkRecord("t", 0, keySchema, key, noOpSchema, value, 0L);

        assertThrows(InvalidStructException.class, () -> mapper().toRow(record, "block-1"));
    }
}
