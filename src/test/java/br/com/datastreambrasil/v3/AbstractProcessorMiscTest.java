package br.com.datastreambrasil.v3;

import org.apache.kafka.common.config.AbstractConfig;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

class AbstractProcessorMiscTest {

    private CdcDbzSchemaProcessor processor(boolean processMultiTables, String tableName) {
        var p = new CdcDbzSchemaProcessor();
        p.connection = mock(Connection.class);
        p.tableName = tableName;
        p.processMultiTables = processMultiTables;
        p.ignoreColumns = new ArrayList<>();
        p.excludeIngestAdditionalFields = new ArrayList<>();
        return p;
    }

    // ── extractTableName ─────────────────────────────────────────────────────

    @Test
    void extractTableName_multiTables_dotSeparatedTopic_returnsLastSegment() {
        var p = processor(true, "DEFAULT_TABLE");
        assertEquals("orders", p.extractTableName("server.schema.orders"));
    }

    @Test
    void extractTableName_multiTables_singleDotTopic_returnsLastSegment() {
        var p = processor(true, "DEFAULT_TABLE");
        assertEquals("orders", p.extractTableName("schema.orders"));
    }

    @Test
    void extractTableName_multiTables_topicWithoutDot_returnsConfiguredTableName() {
        var p = processor(true, "DEFAULT_TABLE");
        assertEquals("DEFAULT_TABLE", p.extractTableName("no_dot_topic"));
    }

    @Test
    void extractTableName_multiTables_nullTopic_returnsConfiguredTableName() {
        var p = processor(true, "DEFAULT_TABLE");
        assertEquals("DEFAULT_TABLE", p.extractTableName(null));
    }

    @Test
    void extractTableName_singleTable_topicWithDot_returnsConfiguredTableName() {
        var p = processor(false, "DEFAULT_TABLE");
        assertEquals("DEFAULT_TABLE", p.extractTableName("server.schema.orders"));
    }

    // ── configParameters ─────────────────────────────────────────────────────

    @Test
    void configParameters_mapsAllConfigFieldsToProcessorState() {
        var p = new CdcDbzSchemaProcessor();
        p.ignoreColumns = new ArrayList<>();
        p.excludeIngestAdditionalFields = new ArrayList<>();
        p.timestampFieldsConvert = new ArrayList<>();
        p.dateFieldsConvert = new ArrayList<>();
        p.timeFieldsConvert = new ArrayList<>();

        var config = new AbstractConfig(SnowflakeSinkConnector.CONFIG_DEF, Map.of(
                "schema", "MY_SCHEMA",
                "table", "MY_TABLE",
                "stage", "MY_STAGE",
                "timestamp_fields_convert", "created_at,updated_at",
                "date_fields_convert", "birth_date",
                "time_fields_convert", "start_time",
                "ignore_columns", "DELETED_AT",
                "process_multiples_tables", "true",
                "find_columns_in_metadata", "true",
                "must_process_read_only_messages", "false"
        ));

        p.configParameters(config);

        assertEquals("MY_SCHEMA", p.schemaName);
        assertEquals("MY_TABLE", p.tableName);
        assertEquals("MY_STAGE", p.stageName);
        assertEquals(List.of("created_at", "updated_at"), p.timestampFieldsConvert);
        assertEquals(List.of("birth_date"), p.dateFieldsConvert);
        assertEquals(List.of("start_time"), p.timeFieldsConvert);
        assertEquals(List.of("DELETED_AT"), p.ignoreColumns);
        assertEquals(true, p.processMultiTables);
        assertEquals(true, p.findInColumnsMetadata);
        assertEquals(false, p.mustProcessReadOnlyMessages);
    }
}
