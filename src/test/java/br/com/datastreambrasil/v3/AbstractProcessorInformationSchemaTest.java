package br.com.datastreambrasil.v3;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AbstractProcessorInformationSchemaTest {

    @Test
    void testUsesStatementNotPreparedStatement() throws Exception {
        var processor = createProcessor("MY_SCHEMA", "MY_TABLE_INGEST");
        var mockStatement = mock(Statement.class);
        var mockResultSet = buildResultSet(List.of("COL1"));

        when(processor.connection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);

        processor.configMetadataV2();

        verify(processor.connection).createStatement();
        verify(processor.connection, never()).prepareStatement(anyString());
    }

    @Test
    void testSqlContainsUppercaseSchemaAndTable() throws Exception {
        var processor = createProcessor("my_schema", "my_table_INGEST");
        var mockStatement = mock(Statement.class);
        var mockResultSet = buildResultSet(List.of("COL1"));

        when(processor.connection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);

        processor.configMetadataV2();

        verify(mockStatement).executeQuery(argThat(sql ->
                sql.contains("'MY_SCHEMA'") && sql.contains("'MY_TABLE_INGEST'")));
    }

    @Test
    void testColumnsIngestTableAndFinalTablePopulated() throws Exception {
        var ingestColumns = List.of("COL1", "COL2", "IH_TOPIC", "IH_PARTITION", "IH_OFFSET", "IH_OP", "IH_DATETIME", "IH_BLOCKID");
        var processor = createProcessor("MY_SCHEMA", "MY_TABLE_INGEST");
        var mockStatement = mock(Statement.class);
        var mockResultSet = buildResultSet(ingestColumns);

        when(processor.connection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);

        processor.configMetadataV2();

        assertEquals(ingestColumns, processor.columnsIngestTable);
        assertEquals(List.of("COL1", "COL2"), processor.columnsFinalTable);
    }

    @Test
    void testThrowsExceptionWhenNoColumnsFound() throws Exception {
        var processor = createProcessor("MY_SCHEMA", "MY_TABLE_INGEST");
        var mockStatement = mock(Statement.class);
        var mockResultSet = buildResultSet(List.of());

        when(processor.connection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);

        assertThrows(RuntimeException.class, processor::configMetadataV2);
    }

    @Test
    void testIgnoreColumnsAreFiltered() throws Exception {
        var processor = createProcessor("MY_SCHEMA", "MY_TABLE_INGEST");
        processor.ignoreColumns.add("COL2");
        var mockStatement = mock(Statement.class);
        var mockResultSet = buildResultSet(List.of("COL1", "COL2", "COL3"));

        when(processor.connection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);

        processor.configMetadataV2();

        assertEquals(List.of("COL1", "COL3"), processor.columnsIngestTable);
    }

    private CdcDbzSchemaProcessor createProcessor(String schemaName, String ingestTableName) {
        var processor = new CdcDbzSchemaProcessor();
        processor.connection = mock(Connection.class);
        processor.schemaName = schemaName;
        processor.ingestTableName = ingestTableName;
        processor.ignoreColumns = new ArrayList<>();
        return processor;
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
