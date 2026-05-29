package br.com.datastreambrasil.v3;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests AbstractProcessor's lazy column loading via both the JDBC metadata API
 * and the INFORMATION_SCHEMA query path.
 */
class AbstractProcessorInformationSchemaTest {

    @Test
    void testKnownIngestTablesPopulatedOnGetOrCreateBuffer() {
        var processor = createProcessor("MY_SCHEMA", "MY_TABLE");
        processor.bufferInitialCapacity = 100;

        processor.getOrCreateBuffer("MY_TABLE");

        assertTrue(processor.knownIngestTables.contains("MY_TABLE_INGEST"),
                "knownIngestTables should contain MY_TABLE_INGEST after buffer creation");
    }

    private CdcDbzSchemaProcessor createProcessor(String schemaName, String tableBaseName) {
        var processor = new CdcDbzSchemaProcessor();
        processor.connection = mock(Connection.class);
        processor.schemaName = schemaName;
        processor.tableName = tableBaseName;
        processor.ignoreColumns = new ArrayList<>();
        processor.excludeIngestAdditionalFields = List.of("IH_TOPIC", "IH_PARTITION", "IH_OFFSET", "IH_OP", "IH_DATETIME", "IH_BLOCKID");
        return processor;
    }
}
