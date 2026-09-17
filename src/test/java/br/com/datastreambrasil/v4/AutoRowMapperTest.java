package br.com.datastreambrasil.v4;

import br.com.datastreambrasil.v4.exception.InvalidStructException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AutoRowMapperTest {

    private static final List<String> INGEST_COLUMNS = List.of(
            "ID", "NAME", "IH_TOPIC", "IH_PARTITION", "IH_OFFSET", "IH_OP", "IH_DATETIME", "IH_BLOCKID");

    private static Schema payloadSchema;
    private static Schema envelopeSchema;
    private static Schema keySchema;

    @BeforeAll
    static void beforeAll() {
        payloadSchema = SchemaBuilder.struct()
                .field("Id", Schema.STRING_SCHEMA)
                .field("Name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        envelopeSchema = SchemaBuilder.struct()
                .field("before", payloadSchema)
                .field("after", payloadSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();
        keySchema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).build();
    }

    private AutoRowMapper mapper() {
        return new AutoRowMapper(INGEST_COLUMNS, List.of(), List.of(), List.of(), List.of(), "op");
    }

    /** A Debezium record: the envelope in the value, the operation in its 'op' field. */
    private SinkRecord debezium(String op, String id, long offset) {
        var payload = new Struct(payloadSchema).put("Id", id).put("Name", "Name " + id);
        var value = new Struct(envelopeSchema).put("op", op);
        if ("d".equals(op)) {
            value.put("before", payload);
        } else {
            value.put("after", payload);
        }
        return new SinkRecord("t", 0, keySchema, new Struct(keySchema).put("id", id),
                envelopeSchema, value, offset);
    }

    /** A flat JSON record: the row in the value, the operation in a header. */
    private SinkRecord flat(String op, String id, long offset) {
        var value = new LinkedHashMap<String, Object>();
        value.put("ID", id);
        value.put("NAME", "Name " + id);
        var record = new SinkRecord("t", 0, null, Map.of("ID", id), null,
                "d".equals(op) ? null : value, offset);
        record.headers().addString("op", op);
        return record;
    }

    @Test
    void aDebeziumEnvelopeIsReadAsOne() {
        var row = mapper().toRow(debezium("c", "1", 0L), "block-1");

        assertEquals("1", row.get("ID"));
        assertEquals("Name 1", row.get("NAME"));
        assertEquals("c", row.get("IH_OP"));
    }

    @Test
    void aFlatRowIsReadAsOne() {
        var row = mapper().toRow(flat("c", "2", 1L), "block-1");

        assertEquals("2", row.get("ID"));
        assertEquals("Name 2", row.get("NAME"));
        assertEquals("c", row.get("IH_OP"));
    }

    @Test
    void bothFormatsOnTheSameMapperAndTheSameTopic() {
        var mapper = mapper();

        assertEquals("1", mapper.toRow(debezium("c", "1", 0L), "b").get("ID"));
        assertEquals("2", mapper.toRow(flat("u", "2", 1L), "b").get("ID"));
        assertEquals("3", mapper.toRow(debezium("u", "3", 2L), "b").get("ID"));
    }

    @Test
    void aDebeziumDeleteStillComesFromBefore() {
        var row = mapper().toRow(debezium("d", "7", 0L), "block-1");

        assertEquals("d", row.get("IH_OP"));
        assertEquals("7", row.get("ID"));
        assertEquals("Name 7", row.get("NAME"));
    }

    @Test
    void aFlatDeleteStillComesFromTheKey() {
        var row = mapper().toRow(flat("d", "9", 0L), "block-1");

        assertEquals("d", row.get("IH_OP"));
        assertEquals("9", row.get("ID"));
        assertFalse(row.containsKey("NAME"), "a flat delete carries no value");
    }

    @Test
    void aFlatStructIsNotMistakenForAnEnvelope() {
        // same converter as Debezium, but the value is the row itself
        var schema = SchemaBuilder.struct()
                .field("ID", Schema.STRING_SCHEMA)
                .field("NAME", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        var value = new Struct(schema).put("ID", "5").put("NAME", "Name 5");
        var record = new SinkRecord("t", 0, null, null, schema, value, 0L);
        record.headers().addString("op", "c");

        var row = mapper().toRow(record, "block-1");

        assertEquals("5", row.get("ID"));
        assertEquals("c", row.get("IH_OP"));
    }

    @Test
    void aStructWithOpButNoBeforeOrAfterIsAFlatRow() {
        // a row that happens to have a column called OP is still a row
        var schema = SchemaBuilder.struct()
                .field("ID", Schema.STRING_SCHEMA)
                .field("op", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        var value = new Struct(schema).put("ID", "6").put("op", "whatever");
        var record = new SinkRecord("t", 0, null, null, schema, value, 0L);
        record.headers().addString("op", "r");

        assertEquals("r", mapper().toRow(record, "block-1").get("IH_OP"));
    }

    @Test
    void primaryKeysComeFromWhicheverFormatTheRecordIs() {
        assertEquals(List.of("id"), mapper().extractPk(debezium("c", "1", 0L)));
        assertEquals(List.of("ID"), mapper().extractPk(flat("c", "1", 0L)));
    }

    @Test
    void aFlatRecordWithoutTheOperationHeaderIsStillRejected() {
        var record = new SinkRecord("t", 0, null, Map.of("ID", "1"), null, Map.of("ID", "1"), 0L);

        assertThrows(InvalidStructException.class, () -> mapper().toRow(record, "block-1"));
    }
}
