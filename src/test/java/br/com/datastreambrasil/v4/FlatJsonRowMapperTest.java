package br.com.datastreambrasil.v4;

import br.com.datastreambrasil.v4.exception.InvalidStructException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlatJsonRowMapperTest {

    private static final String TOPIC = "inthub.dbo.pedidos";

    /** Column names as Snowflake reports them for unquoted identifiers. */
    private static final List<String> INGEST_COLUMNS = List.of(
            "ID", "CLIENTE", "TOTAL", "ATUALIZADO_EM", "ITENS",
            "IH_TOPIC", "IH_PARTITION", "IH_OFFSET", "IH_OP", "IH_DATETIME", "IH_BLOCKID",
            "IH_SCHEMA", "IH_TABLE");

    private FlatJsonRowMapper mapper() {
        return new FlatJsonRowMapper(INGEST_COLUMNS, List.of(), List.of(), List.of(), List.of(), "op");
    }

    private SinkRecord record(String op, Object key, Object value, long offset) {
        var record = new SinkRecord(TOPIC, 3, null, key, null, value, offset);
        if (op != null) {
            record.headers().addString("op", op);
        }
        return record;
    }

    /** The value as JsonConverter with schemas.enable=false hands it over: a plain Map. */
    private Map<String, Object> pedido(Object id, String cliente) {
        var value = new LinkedHashMap<String, Object>();
        value.put("ID", id);
        value.put("CLIENTE", cliente);
        value.put("TOTAL", new BigDecimal("249.90"));
        value.put("ATUALIZADO_EM", "2026-07-26T11:02:31Z");
        return value;
    }

    @Test
    void flatJsonValueBecomesTheRowAndMetadataIsStamped() {
        var row = mapper().toRow(record("c", Map.of("ID", 42), pedido(42, "ACME"), 7L), "block-1");

        assertEquals(42, row.get("ID"));
        assertEquals("ACME", row.get("CLIENTE"));
        assertEquals(new BigDecimal("249.90"), row.get("TOTAL"));
        assertEquals("2026-07-26T11:02:31Z", row.get("ATUALIZADO_EM"));
        assertEquals(TOPIC, row.get("IH_TOPIC"));
        assertEquals(3, row.get("IH_PARTITION"));
        assertEquals(7L, row.get("IH_OFFSET"));
        assertEquals("c", row.get("IH_OP"));
        assertEquals("block-1", row.get("IH_BLOCKID"));
        assertTrue(row.containsKey("IH_DATETIME"));

        // the pipe matches by column name, so every key must be a real ingest column
        assertTrue(INGEST_COLUMNS.containsAll(row.keySet()),
                "unexpected columns emitted: " + row.keySet());
    }

    @Test
    void fieldNamesAreMatchedCaseInsensitively() {
        var value = new LinkedHashMap<String, Object>();
        value.put("id", 42);
        value.put("Cliente", "ACME");

        var row = mapper().toRow(record("c", null, value, 0L), "block-1");

        assertEquals(42, row.get("ID"));
        assertEquals("ACME", row.get("CLIENTE"));
    }

    @Test
    void deleteTakesTheRowFromTheKeyBecauseTheValueIsEmpty() {
        var row = mapper().toRow(record("d", Map.of("ID", 42), null, 9L), "block-1");

        assertEquals("d", row.get("IH_OP"));
        assertEquals(42, row.get("ID"));
        assertFalse(row.containsKey("CLIENTE"));
    }

    @Test
    void deletePrefersTheKeyWhenTheProducerAlsoSendsTheRow() {
        var row = mapper().toRow(
                record("d", Map.of("ID", 42), pedido(99, "STALE"), 9L), "block-1");

        assertEquals(42, row.get("ID"));
    }

    @Test
    void deleteWithoutKeyOrValueIsRejected() {
        var record = record("d", null, null, 0L);
        assertThrows(InvalidStructException.class, () -> mapper().toRow(record, "block-1"));
    }

    @Test
    void anOperationOtherThanDeleteNeedsAValue() {
        var record = record("u", Map.of("ID", 42), null, 0L);
        assertThrows(InvalidStructException.class, () -> mapper().toRow(record, "block-1"));
    }

    @Test
    void jsonTextIsParsedSoStringConverterWorks() {
        var value = "{\"ID\":42,\"CLIENTE\":\"ACME\",\"TOTAL\":249.90}";

        var row = mapper().toRow(record("c", "{\"ID\":42}", value, 0L), "block-1");

        assertEquals(42, row.get("ID"));
        assertEquals("ACME", row.get("CLIENTE"));
        // a money column must not pick up a binary rounding error on the way in
        assertEquals(new BigDecimal("249.90"), row.get("TOTAL"));
    }

    @Test
    void jsonBytesAreParsedSoByteArrayConverterWorks() {
        var value = "{\"ID\":42,\"CLIENTE\":\"ACME\"}".getBytes(StandardCharsets.UTF_8);

        var row = mapper().toRow(record("c", null, value, 0L), "block-1");

        assertEquals(42, row.get("ID"));
        assertEquals("ACME", row.get("CLIENTE"));
    }

    @Test
    void aFlatStructIsAcceptedToo() {
        var schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("cliente", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        var value = new Struct(schema).put("id", 42).put("cliente", "ACME");
        var record = new SinkRecord(TOPIC, 3, null, null, schema, value, 0L);
        record.headers().addString("op", "c");

        var row = mapper().toRow(record, "block-1");

        assertEquals(42, row.get("ID"));
        assertEquals("ACME", row.get("CLIENTE"));
    }

    @Test
    void valueThatIsNotJsonIsRejectedWithAnActionableMessage() {
        var record = record("c", null, "ID=42;CLIENTE=ACME", 0L);

        var e = assertThrows(InvalidStructException.class, () -> mapper().toRow(record, "block-1"));
        assertTrue(e.getMessage().contains("not a flat JSON object"), e.getMessage());
    }

    @Test
    void missingOperationHeaderIsRejected() {
        var record = record(null, Map.of("ID", 42), pedido(42, "ACME"), 0L);

        var e = assertThrows(InvalidStructException.class, () -> mapper().toRow(record, "block-1"));
        assertTrue(e.getMessage().contains("op"), e.getMessage());
    }

    @Test
    void unknownOperationIsRejected() {
        var record = record("x", Map.of("ID", 42), pedido(42, "ACME"), 0L);

        var e = assertThrows(InvalidStructException.class, () -> mapper().toRow(record, "block-1"));
        assertTrue(e.getMessage().contains("r, c, u or d"), e.getMessage());
    }

    @Test
    void theOperationIsLowerCasedBecauseTheMergeComparesLowerCaseLiterals() {
        var row = mapper().toRow(record("U", Map.of("ID", 42), pedido(42, "ACME"), 0L), "block-1");

        assertEquals("u", row.get("IH_OP"));
    }

    @Test
    void theHeaderNameIsMatchedCaseInsensitively() {
        var record = new SinkRecord(TOPIC, 3, null, null, null, pedido(42, "ACME"), 0L);
        record.headers().addString("OP", "c");

        assertEquals("c", mapper().toRow(record, "block-1").get("IH_OP"));
    }

    @Test
    void theOperationHeaderNameIsConfigurable() {
        var mapper = new FlatJsonRowMapper(INGEST_COLUMNS, List.of(), List.of(), List.of(),
                List.of(), "__op");
        var record = new SinkRecord(TOPIC, 3, null, null, null, pedido(42, "ACME"), 0L);
        record.headers().addString("__op", "r");

        assertEquals("r", mapper.toRow(record, "block-1").get("IH_OP"));
    }

    @Test
    void sourceSchemaAndTableAreStampedWhenTheIngestTableHasTheColumns() {
        var record = record("c", null, pedido(42, "ACME"), 0L);
        record.headers().addString("schema", "dbo");
        record.headers().addString("table", "pedidos");

        var row = mapper().toRow(record, "block-1");

        assertEquals("dbo", row.get("IH_SCHEMA"));
        assertEquals("pedidos", row.get("IH_TABLE"));
    }

    @Test
    void withoutThoseHeadersTheColumnsAreSimplyLeftOut() {
        var row = mapper().toRow(record("c", null, pedido(42, "ACME"), 0L), "block-1");

        assertFalse(row.containsKey("IH_SCHEMA"));
        assertFalse(row.containsKey("IH_TABLE"));
    }

    @Test
    void nestedObjectsBecomeJsonTextRatherThanAJavaToString() {
        var value = new LinkedHashMap<String, Object>();
        value.put("ID", 42);
        value.put("ITENS", List.of(Map.of("SKU", "A1")));

        var row = mapper().toRow(record("c", null, value, 0L), "block-1");

        assertEquals("[{\"SKU\":\"A1\"}]", row.get("ITENS"));
    }

    @Test
    void nullFieldsAreOmittedSoTheyLandAsNull() {
        var value = new LinkedHashMap<String, Object>();
        value.put("ID", 42);
        value.put("CLIENTE", null);

        var row = mapper().toRow(record("c", null, value, 0L), "block-1");

        assertFalse(row.containsKey("CLIENTE"));
    }

    @Test
    void columnsAbsentFromTheIngestTableAreNeverEmitted() {
        var narrow = new FlatJsonRowMapper(List.of("ID", "IH_OP"), List.of(), List.of(), List.of(),
                List.of(), "op");

        var row = narrow.toRow(record("c", null, pedido(42, "ACME"), 0L), "block-1");

        assertEquals(2, row.size());
        assertEquals(42, row.get("ID"));
        assertEquals("c", row.get("IH_OP"));
    }

    @Test
    void aTemporalColumnSentAsTextIsPassedThroughInsteadOfFailingTheTask() {
        // epoch-millis conversion is configured for ATUALIZADO_EM, but flat JSON usually carries
        // an ISO string: Snowflake parses that itself, and the task must not die on it
        var mapper = new FlatJsonRowMapper(INGEST_COLUMNS, List.of(), List.of("ATUALIZADO_EM"),
                List.of(), List.of(), "op");

        var row = mapper.toRow(record("c", null, pedido(42, "ACME"), 0L), "block-1");

        assertEquals("2026-07-26T11:02:31Z", row.get("ATUALIZADO_EM"));
    }

    @Test
    void theTemporalConversionsStillApplyToEpochNumbers() {
        var mapper = new FlatJsonRowMapper(INGEST_COLUMNS, List.of(), List.of("ATUALIZADO_EM"),
                List.of(), List.of(), "op");
        var value = new LinkedHashMap<String, Object>();
        value.put("ID", 42);
        value.put("ATUALIZADO_EM", 1753527751000L);

        var row = mapper.toRow(record("c", null, value, 0L), "block-1");

        assertFalse(row.get("ATUALIZADO_EM") instanceof Number,
                "an epoch-millis column must still be converted");
    }

    @Test
    void thePrimaryKeyComesFromTheKeyColumns() {
        var key = new LinkedHashMap<String, Object>();
        key.put("PEDIDO_ID", 42);
        key.put("ITEM_ID", 1);

        assertEquals(List.of("PEDIDO_ID", "ITEM_ID"),
                mapper().extractPk(record("u", key, pedido(42, "ACME"), 0L)));
    }

    @Test
    void thePrimaryKeyIsUpperCasedToMatchTheIngestColumns() {
        assertEquals(List.of("ID"),
                mapper().extractPk(record("u", Map.of("id", 42), pedido(42, "ACME"), 0L)));
    }

    @Test
    void withoutAKeyThePrimaryKeyIsUnknownRatherThanAnError() {
        assertEquals(List.of(), mapper().extractPk(record("c", null, pedido(42, "ACME"), 0L)));
    }

    @Test
    void configuredPkFieldsWinOverTheRecordKey() {
        var mapper = new FlatJsonRowMapper(INGEST_COLUMNS, List.of("cliente"), List.of(), List.of(),
                List.of(), "op");

        assertEquals(List.of("CLIENTE"),
                mapper.extractPk(record("c", Map.of("ID", 42), pedido(42, "ACME"), 0L)));
    }
}
