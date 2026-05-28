package br.com.datastreambrasil.v3;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests AbstractProcessor's lazy column loading via both the JDBC metadata API
 * and the INFORMATION_SCHEMA query path.
 */
class AbstractProcessorInformationSchemaTest {

    @Test
    void testUsesMetadataApiNotStatement() throws Exception {
        var processor = createProcessor("MY_SCHEMA", "MY_TABLE");
        processor.findInColumnsMetadata = true;
        var dbMeta = mock(DatabaseMetaData.class);
        var rsIngest = buildResultSet(List.of("COL1", "IH_TOPIC"));
        when(processor.connection.getMetaData()).thenReturn(dbMeta);
        when(dbMeta.getColumns(any(), any(), eq("MY_TABLE_INGEST"), any())).thenReturn(rsIngest);

        processor.ensureColumnsForTable("MY_TABLE");

        verify(processor.connection, atLeastOnce()).getMetaData();
        verify(processor.connection, never()).createStatement();
        verify(processor.connection, never()).prepareStatement(anyString());
    }

    @Test
    void testUsesStatementWhenFindColumnsMetadataIsFalse() throws Exception {
        var processor = createProcessor("MY_SCHEMA", "MY_TABLE");
        processor.findInColumnsMetadata = false;
        mockStatementForColumns(processor, List.of("COL1", "IH_TOPIC"));

        processor.ensureColumnsForTable("MY_TABLE");

        verify(processor.connection, atLeastOnce()).createStatement();
        verify(processor.connection, never()).getMetaData();
    }

    @Test
    void testColumnsLoadedOnlyOnceForSameTable() throws Exception {
        var processor = createProcessor("MY_SCHEMA", "MY_TABLE");
        mockStatementForColumns(processor, List.of("COL1", "IH_TOPIC"));

        processor.ensureColumnsForTable("MY_TABLE");
        processor.ensureColumnsForTable("MY_TABLE"); // second call — must hit cache

        // createStatement called only once: only on the first invocation
        verify(processor.connection, times(1)).createStatement();
    }

    @Test
    void testColumnsIngestTableAndFinalTablePopulated() throws Exception {
        var ingestColumns = List.of("COL1", "COL2", "IH_TOPIC", "IH_PARTITION", "IH_OFFSET", "IH_OP", "IH_DATETIME", "IH_BLOCKID");
        var processor = createProcessor("MY_SCHEMA", "MY_TABLE");
        mockStatementForColumns(processor, ingestColumns);

        processor.ensureColumnsForTable("MY_TABLE");

        assertEquals(ingestColumns, processor.columnsIngestTable.get("MY_TABLE"));
        // finalTable is derived from ingestTable by filtering out excludeIngestAdditionalFields
        assertEquals(List.of("COL1", "COL2"), processor.columnsFinalTable.get("MY_TABLE"));
    }

    @Test
    void testThrowsExceptionWhenNoColumnsFound() throws Exception {
        var processor = createProcessor("MY_SCHEMA", "MY_TABLE");
        mockStatementForColumns(processor, List.of());

        assertThrows(RuntimeException.class, () -> processor.ensureColumnsForTable("MY_TABLE"));
    }

    @Test
    void testIgnoreColumnsAreFiltered() throws Exception {
        var processor = createProcessor("MY_SCHEMA", "MY_TABLE");
        processor.ignoreColumns.add("COL2");
        mockStatementForColumns(processor, List.of("COL1", "COL2", "COL3"));

        processor.ensureColumnsForTable("MY_TABLE");

        assertEquals(List.of("COL1", "COL3"), processor.columnsIngestTable.get("MY_TABLE"));
    }

    @Test
    void testPreloadAllIngestTableColumnsPopulatesCache() throws Exception {
        var processor = createProcessor("MY_SCHEMA", "ANY_TABLE");
        var rows = List.of(
                new String[]{"TABLE_A_INGEST", "COL1"},
                new String[]{"TABLE_A_INGEST", "COL2"},
                new String[]{"TABLE_A_INGEST", "IH_TOPIC"},
                new String[]{"TABLE_B_INGEST", "COL3"},
                new String[]{"TABLE_B_INGEST", "IH_OP"}
        );
        mockBulkStatementForColumns(processor, rows);

        processor.preloadAllIngestTableColumns();

        assertEquals(List.of("COL1", "COL2", "IH_TOPIC"), processor.columnsIngestTable.get("TABLE_A"));
        assertEquals(List.of("COL1", "COL2"), processor.columnsFinalTable.get("TABLE_A"));
        assertEquals(List.of("COL3", "IH_OP"), processor.columnsIngestTable.get("TABLE_B"));
        assertEquals(List.of("COL3"), processor.columnsFinalTable.get("TABLE_B"));
    }

    @Test
    void testPreloadAllIngestTableColumnsEmptySchema() throws Exception {
        var processor = createProcessor("MY_SCHEMA", "ANY_TABLE");
        mockBulkStatementForColumns(processor, List.of());

        processor.preloadAllIngestTableColumns();

        assertTrue(processor.columnsIngestTable.isEmpty());
    }

    @Test
    void testKnownIngestTablesPopulatedOnGetOrCreateBuffer() {
        var processor = createProcessor("MY_SCHEMA", "MY_TABLE");
        processor.bufferInitialCapacity = 100;

        processor.getOrCreateBuffer("MY_TABLE");

        assertTrue(processor.knownIngestTables.contains("MY_TABLE_INGEST"),
                "knownIngestTables should contain MY_TABLE_INGEST after buffer creation");
    }

    @Test
    void testExtractTableNameFromTopicWithDot() {
        var processor = createProcessor("MY_SCHEMA", "MY_TABLE");
        processor.processMultiTables = true;
        assertEquals("tabelaX", processor.extractTableNameFromTopic("compras.loja-b.tabelaX"));
        assertEquals("tabelaZ", processor.extractTableNameFromTopic("compras.loja-c.tabelaZ"));
    }

    @Test
    void testExtractTableNameFromTopicWithoutDotFallsBackToTableName() {
        var processor = createProcessor("MY_SCHEMA", "MY_TABLE");
        assertEquals("MY_TABLE", processor.extractTableNameFromTopic("no_dot_topic"));
        assertEquals("MY_TABLE", processor.extractTableNameFromTopic(null));
    }

    @Test
    void testExtractTableNameFromTopicWithDotButSingleTableMode() {
        var processor = createProcessor("MY_SCHEMA", "MY_TABLE");
        processor.processMultiTables = false;
        assertEquals("MY_TABLE", processor.extractTableNameFromTopic("compras.loja-b.tabelaX"),
                "Single-table mode must ignore topic and always use tableName");
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

    private Statement mockBulkStatementForColumns(CdcDbzSchemaProcessor processor, List<String[]> rows) throws Exception {
        var stmtMock = mock(Statement.class);
        var rsMock = mock(ResultSet.class);
        var idx = new int[]{-1};
        when(rsMock.next()).thenAnswer(a -> {
            idx[0]++;
            return idx[0] < rows.size();
        });
        when(rsMock.getString("TABLE_NAME")).thenAnswer(a -> rows.get(idx[0])[0]);
        when(rsMock.getString("COLUMN_NAME")).thenAnswer(a -> rows.get(idx[0])[1]);
        when(processor.connection.createStatement()).thenReturn(stmtMock);
        when(stmtMock.executeQuery(any())).thenReturn(rsMock);
        return stmtMock;
    }

    private Statement mockStatementForColumns(CdcDbzSchemaProcessor processor, List<String> columns) throws Exception {
        var stmtMock = mock(Statement.class);
        var rsMock = buildResultSet(columns);
        when(processor.connection.createStatement()).thenReturn(stmtMock);
        when(stmtMock.executeQuery(any())).thenReturn(rsMock);
        return stmtMock;
    }

    private ResultSet buildResultSet(List<String> columns) throws Exception {
        var rs = mock(ResultSet.class);
        var idx = new int[]{-1};
        when(rs.next()).thenAnswer(a -> {
            idx[0]++;
            return idx[0] < columns.size();
        });
        if (!columns.isEmpty()) {
            when(rs.getString("COLUMN_NAME")).thenAnswer(a -> columns.get(idx[0]));
        }
        return rs;
    }
}
