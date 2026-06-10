package br.com.datastreambrasil.v3;

import org.apache.kafka.common.config.AbstractConfig;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AbstractProcessorColumnLoadingTest {

    // ── helpers ──────────────────────────────────────────────────────────────

    private CdcDbzSchemaProcessor processor(String schema) {
        var p = new CdcDbzSchemaProcessor();
        p.connection = mock(Connection.class);
        p.schemaName = schema;
        p.tableName = "MY_TABLE";
        p.ignoreColumns = new ArrayList<>();
        p.excludeIngestAdditionalFields = new ArrayList<>();
        return p;
    }

    /** ResultSet that iterates over a list of column names (COLUMN_NAME only). */
    private ResultSet rsFromColumns(List<String> cols) throws SQLException {
        var rs = mock(ResultSet.class);
        var idx = new int[]{-1};
        when(rs.next()).thenAnswer(a -> ++idx[0] < cols.size());
        when(rs.getString("COLUMN_NAME")).thenAnswer(a -> cols.get(idx[0]));
        return rs;
    }

    /** ResultSet that iterates over [TABLE_NAME, COLUMN_NAME] pairs. */
    private ResultSet rsFromRows(List<String[]> rows) throws SQLException {
        var rs = mock(ResultSet.class);
        var idx = new int[]{-1};
        when(rs.next()).thenAnswer(a -> ++idx[0] < rows.size());
        when(rs.getString("TABLE_NAME")).thenAnswer(a -> rows.get(idx[0])[0]);
        when(rs.getString("COLUMN_NAME")).thenAnswer(a -> rows.get(idx[0])[1]);
        return rs;
    }

    private AbstractConfig configWithExcludeFields(List<String> excludeFields) {
        return new AbstractConfig(SnowflakeSinkConnector.CONFIG_DEF, Map.of(
                "schema", "MY_SCHEMA",
                "table", "MY_TABLE",
                "stage", "MY_STAGE",
                "exclude_ingest_additional_fields", String.join(",", excludeFields)
        ));
    }

    private AbstractConfig defaultConfig() {
        return new AbstractConfig(SnowflakeSinkConnector.CONFIG_DEF, Map.of(
                "schema", "MY_SCHEMA",
                "table", "MY_TABLE",
                "stage", "MY_STAGE"
        ));
    }

    // ── prepareColumnsFromMetadata ────────────────────────────────────────────

    @Test
    void prepareColumnsFromMetadata_populatesBothMapsAndKnownIngestTables() throws SQLException {
        var p = processor("MY_SCHEMA");
        var cols = List.of("ID", "NAME", "CREATED_AT");
        var dbMeta = mock(DatabaseMetaData.class);
        var rs = rsFromColumns(cols);
        when(p.connection.getMetaData()).thenReturn(dbMeta);
        when(dbMeta.getColumns(isNull(), eq("MY_SCHEMA"), eq("MY_TABLE"), isNull())).thenReturn(rs);

        p.prepareColumnsFromMetadata("MY_TABLE", configWithExcludeFields(List.of()));

        assertEquals(cols, p.columnsFinalTable.get("MY_TABLE"),
                "columnsFinalTable should contain exactly the metadata columns");
        assertTrue(p.columnsIngestTable.get("MY_TABLE_INGEST").containsAll(cols),
                "columnsIngestTable should include all metadata columns");
        assertTrue(p.knownIngestTables.contains("MY_TABLE_INGEST"),
                "knownIngestTables should be updated with the ingest table name");
    }

    @Test
    void prepareColumnsFromMetadata_ignoreColumnsRemovedFromBothMaps() throws SQLException {
        var p = processor("MY_SCHEMA");
        p.ignoreColumns.add("AUDIT_COL");
        var dbMeta = mock(DatabaseMetaData.class);
        var rs = rsFromColumns(List.of("ID", "NAME", "AUDIT_COL"));
        when(p.connection.getMetaData()).thenReturn(dbMeta);
        when(dbMeta.getColumns(isNull(), eq("MY_SCHEMA"), eq("MY_TABLE"), isNull())).thenReturn(rs);

        p.prepareColumnsFromMetadata("MY_TABLE", configWithExcludeFields(List.of()));

        assertFalse(p.columnsFinalTable.get("MY_TABLE").contains("AUDIT_COL"),
                "ignoreColumns should be removed from final table");
        assertFalse(p.columnsIngestTable.get("MY_TABLE_INGEST").contains("AUDIT_COL"),
                "ignoreColumns should be removed from ingest table");
        assertTrue(p.columnsFinalTable.get("MY_TABLE").containsAll(List.of("ID", "NAME")));
    }

    @Test
    void prepareColumnsFromMetadata_excludeIngestAdditionalFieldsAddedOnlyToIngestTable() throws SQLException {
        var p = processor("MY_SCHEMA");
        var dbMeta = mock(DatabaseMetaData.class);
        var rs = rsFromColumns(List.of("ID", "NAME"));
        when(p.connection.getMetaData()).thenReturn(dbMeta);
        when(dbMeta.getColumns(isNull(), eq("MY_SCHEMA"), eq("MY_TABLE"), isNull())).thenReturn(rs);

        p.prepareColumnsFromMetadata("MY_TABLE", configWithExcludeFields(List.of("IH_TOPIC", "IH_OFFSET")));

        var finalCols = p.columnsFinalTable.get("MY_TABLE");
        var ingestCols = p.columnsIngestTable.get("MY_TABLE_INGEST");
        assertFalse(finalCols.contains("IH_TOPIC"), "excludeIngestAdditionalFields must not appear in final table");
        assertFalse(finalCols.contains("IH_OFFSET"), "excludeIngestAdditionalFields must not appear in final table");
        assertTrue(ingestCols.contains("IH_TOPIC"), "excludeIngestAdditionalFields must be present in ingest table");
        assertTrue(ingestCols.contains("IH_OFFSET"), "excludeIngestAdditionalFields must be present in ingest table");
    }

    @Test
    void prepareColumnsFromMetadata_emptyMetadataResultThrowsRuntimeException() throws SQLException {
        var p = processor("MY_SCHEMA");
        var dbMeta = mock(DatabaseMetaData.class);
        var rs = rsFromColumns(List.of());
        when(p.connection.getMetaData()).thenReturn(dbMeta);
        when(dbMeta.getColumns(isNull(), eq("MY_SCHEMA"), eq("MY_TABLE"), isNull())).thenReturn(rs);

        assertThrows(RuntimeException.class,
                () -> p.prepareColumnsFromMetadata("MY_TABLE", defaultConfig()));
    }

    @Test
    void prepareColumnsFromMetadata_sqlExceptionWrappedInRuntimeException() throws SQLException {
        var p = processor("MY_SCHEMA");
        when(p.connection.getMetaData()).thenThrow(new SQLException("connection failed"));

        assertThrows(RuntimeException.class,
                () -> p.prepareColumnsFromMetadata("MY_TABLE", defaultConfig()));
    }

    // ── preloadAllTableColumns ────────────────────────────────────────────────

    @Test
    void preloadAllTableColumns_populatesMapsWithoutPopulatingKnownIngestTables() throws SQLException {
        var p = processor("MY_SCHEMA");
        var stmt = mock(Statement.class);
        var rs = rsFromRows(List.of(
                new String[]{"MY_TABLE_INGEST", "ID"},
                new String[]{"MY_TABLE_INGEST", "NAME"}
        ));
        when(p.connection.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(any())).thenReturn(rs);

        p.preloadAllTableColumns();

        assertEquals(List.of("ID", "NAME"), p.columnsIngestTable.get("MY_TABLE_INGEST"),
                "columnsIngestTable should reflect all columns for the ingest table");
        assertEquals(List.of("ID", "NAME"), p.columnsFinalTable.get("MY_TABLE"),
                "columnsFinalTable should use the base table name (without _INGEST suffix)");
        assertTrue(p.knownIngestTables.isEmpty(),
                "preloadAllTableColumns must not populate knownIngestTables; only topics in processing do");
    }

    @Test
    void preloadAllTableColumns_multipleTables_eachTablePopulatedIndependently() throws SQLException {
        var p = processor("MY_SCHEMA");
        var stmt = mock(Statement.class);
        var rs = rsFromRows(List.of(
                new String[]{"TABLE_A_INGEST", "ID"},
                new String[]{"TABLE_A_INGEST", "VALUE"},
                new String[]{"TABLE_B_INGEST", "CODE"},
                new String[]{"TABLE_B_INGEST", "DESCRIPTION"}
        ));
        when(p.connection.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(any())).thenReturn(rs);

        p.preloadAllTableColumns();

        assertEquals(List.of("ID", "VALUE"), p.columnsIngestTable.get("TABLE_A_INGEST"));
        assertEquals(List.of("ID", "VALUE"), p.columnsFinalTable.get("TABLE_A"));
        assertEquals(List.of("CODE", "DESCRIPTION"), p.columnsIngestTable.get("TABLE_B_INGEST"));
        assertEquals(List.of("CODE", "DESCRIPTION"), p.columnsFinalTable.get("TABLE_B"));
        assertTrue(p.knownIngestTables.isEmpty(),
                "preloadAllTableColumns must not populate knownIngestTables; only topics in processing do");
    }

    @Test
    void preloadAllTableColumns_ignoreColumnsRemovedFromBothMaps() throws SQLException {
        var p = processor("MY_SCHEMA");
        p.ignoreColumns.add("AUDIT_COL");
        var stmt = mock(Statement.class);
        var rs = rsFromRows(List.of(
                new String[]{"MY_TABLE_INGEST", "ID"},
                new String[]{"MY_TABLE_INGEST", "AUDIT_COL"},
                new String[]{"MY_TABLE_INGEST", "NAME"}
        ));
        when(p.connection.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(any())).thenReturn(rs);

        p.preloadAllTableColumns();

        assertFalse(p.columnsIngestTable.get("MY_TABLE_INGEST").contains("AUDIT_COL"),
                "ignoreColumns should be removed from ingest table");
        assertFalse(p.columnsFinalTable.get("MY_TABLE").contains("AUDIT_COL"),
                "ignoreColumns should be removed from final table");
        assertTrue(p.columnsIngestTable.get("MY_TABLE_INGEST").containsAll(List.of("ID", "NAME")));
    }

    @Test
    void preloadAllTableColumns_excludeIngestAdditionalFieldsRemovedFromFinalTableOnly() throws SQLException {
        var p = processor("MY_SCHEMA");
        p.excludeIngestAdditionalFields = new ArrayList<>(List.of("IH_TOPIC", "IH_OFFSET"));
        var stmt = mock(Statement.class);
        var rs = rsFromRows(List.of(
                new String[]{"MY_TABLE_INGEST", "ID"},
                new String[]{"MY_TABLE_INGEST", "IH_TOPIC"},
                new String[]{"MY_TABLE_INGEST", "IH_OFFSET"}
        ));
        when(p.connection.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(any())).thenReturn(rs);

        p.preloadAllTableColumns();

        var ingestCols = p.columnsIngestTable.get("MY_TABLE_INGEST");
        var finalCols = p.columnsFinalTable.get("MY_TABLE");
        assertTrue(ingestCols.containsAll(List.of("IH_TOPIC", "IH_OFFSET")),
                "excludeIngestAdditionalFields must remain in ingest table");
        assertFalse(finalCols.contains("IH_TOPIC"),
                "excludeIngestAdditionalFields must be excluded from final table");
        assertFalse(finalCols.contains("IH_OFFSET"),
                "excludeIngestAdditionalFields must be excluded from final table");
        assertTrue(finalCols.contains("ID"), "Non-excluded columns should remain in final table");
    }

    @Test
    void preloadAllTableColumns_excludeIngestAdditionalFieldsCaseInsensitive() throws SQLException {
        var p = processor("MY_SCHEMA");
        p.excludeIngestAdditionalFields = new ArrayList<>(List.of("IH_TOPIC"));
        var stmt = mock(Statement.class);
        // Ingest table column in lowercase while excludeIngestAdditionalFields is uppercase
        var rs = rsFromRows(List.of(
                new String[]{"MY_TABLE_INGEST", "ID"},
                new String[]{"MY_TABLE_INGEST", "ih_topic"}
        ));
        when(p.connection.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(any())).thenReturn(rs);

        p.preloadAllTableColumns();

        assertFalse(p.columnsFinalTable.get("MY_TABLE").contains("ih_topic"),
                "Exclusion comparison should be case-insensitive");
        assertTrue(p.columnsIngestTable.get("MY_TABLE_INGEST").contains("ih_topic"),
                "Ingest table should still retain the column regardless of case");
    }

    @Test
    void preloadAllTableColumns_emptyResultThrowsRuntimeException() throws SQLException {
        var p = processor("MY_SCHEMA");
        var stmt = mock(Statement.class);
        var rs = rsFromRows(List.of());
        when(p.connection.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery(any())).thenReturn(rs);

        assertThrows(RuntimeException.class, p::preloadAllTableColumns);
    }

    @Test
    void preloadAllTableColumns_sqlExceptionWrappedInRuntimeException() throws SQLException {
        var p = processor("MY_SCHEMA");
        when(p.connection.createStatement()).thenThrow(new SQLException("db down"));

        assertThrows(RuntimeException.class, p::preloadAllTableColumns);
    }

    // ── loadColumnsFromConfig ─────────────────────────────────────────────────

    @Test
    void loadColumnsFromConfig_singleTable_populatesBothMapsAndKnownIngestTables() {
        var p = processor("MY_SCHEMA");

        p.loadColumnsFromConfig("MY_TABLE-ID,NAME,CREATED_AT");

        assertEquals(List.of("ID", "NAME", "CREATED_AT"), p.columnsIngestTable.get("MY_TABLE_INGEST"));
        assertEquals(List.of("ID", "NAME", "CREATED_AT"), p.columnsFinalTable.get("MY_TABLE"));
        assertTrue(p.knownIngestTables.contains("MY_TABLE_INGEST"));
    }

    @Test
    void loadColumnsFromConfig_multipleTables_eachTablePopulatedIndependently() {
        var p = processor("MY_SCHEMA");

        p.loadColumnsFromConfig("TABLE_A-ID,VALUE|TABLE_B-CODE,DESCRIPTION");

        assertEquals(List.of("ID", "VALUE"), p.columnsIngestTable.get("TABLE_A_INGEST"));
        assertEquals(List.of("ID", "VALUE"), p.columnsFinalTable.get("TABLE_A"));
        assertEquals(List.of("CODE", "DESCRIPTION"), p.columnsIngestTable.get("TABLE_B_INGEST"));
        assertEquals(List.of("CODE", "DESCRIPTION"), p.columnsFinalTable.get("TABLE_B"));
        assertTrue(p.knownIngestTables.containsAll(List.of("TABLE_A_INGEST", "TABLE_B_INGEST")));
    }

    @Test
    void loadColumnsFromConfig_ignoreColumnsRemovedFromBothMaps() {
        var p = processor("MY_SCHEMA");
        p.ignoreColumns.add("AUDIT_COL");

        p.loadColumnsFromConfig("MY_TABLE-ID,NAME,AUDIT_COL");

        assertFalse(p.columnsIngestTable.get("MY_TABLE_INGEST").contains("AUDIT_COL"),
                "ignoreColumns should be removed from ingest table");
        assertFalse(p.columnsFinalTable.get("MY_TABLE").contains("AUDIT_COL"),
                "ignoreColumns should be removed from final table");
        assertTrue(p.columnsIngestTable.get("MY_TABLE_INGEST").containsAll(List.of("ID", "NAME")));
    }

    @Test
    void loadColumnsFromConfig_tableNameConvertedToUpperCase() {
        var p = processor("MY_SCHEMA");

        p.loadColumnsFromConfig("my_table-ID,NAME");

        assertTrue(p.knownIngestTables.contains("MY_TABLE_INGEST"),
                "Table name should be uppercased");
        assertNotNull(p.columnsFinalTable.get("MY_TABLE"));
    }

    @Test
    void loadColumnsFromConfig_invalidFormatThrowsRuntimeException() {
        var p = processor("MY_SCHEMA");

        assertThrows(RuntimeException.class,
                () -> p.loadColumnsFromConfig("INVALID_ENTRY_WITHOUT_DASH"));
    }
}
