package br.com.datastreambrasil.v2;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SnowflakeSinkTaskInformationSchemaTest {

    @Test
    void testUsesStatementNotPreparedStatement() throws Throwable {
        var task = setupTask("MY_SCHEMA");
        var mockConnection = getField(task, "connection", Connection.class);
        var mockStatement = mock(Statement.class);
        var mockResultSet = buildResultSet(List.of("COL1"));

        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);

        invokeGetColumns(task, "my_table");

        verify(mockConnection).createStatement();
        verify(mockConnection, never()).prepareStatement(anyString());
    }

    @Test
    void testSqlContainsUppercaseSchemaAndTable() throws Throwable {
        var task = setupTask("my_schema");
        var mockConnection = getField(task, "connection", Connection.class);
        var mockStatement = mock(Statement.class);
        var mockResultSet = buildResultSet(List.of("COL1"));

        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);

        invokeGetColumns(task, "my_table");

        verify(mockStatement).executeQuery(argThat(sql ->
                sql.contains("'MY_SCHEMA'") && sql.contains("'MY_TABLE'")));
    }

    @Test
    void testReturnsColumnsFromResultSet() throws Throwable {
        var task = setupTask("MY_SCHEMA");
        var mockConnection = getField(task, "connection", Connection.class);
        var mockStatement = mock(Statement.class);
        var mockResultSet = buildResultSet(List.of("COL1", "COL2", "COL3"));

        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);

        var result = invokeGetColumns(task, "my_table");

        assertEquals(List.of("COL1", "COL2", "COL3"), result);
    }

    @Test
    void testThrowsExceptionWhenNoColumnsFound() throws Throwable {
        var task = setupTask("MY_SCHEMA");
        var mockConnection = getField(task, "connection", Connection.class);
        var mockStatement = mock(Statement.class);
        var mockResultSet = buildResultSet(List.of());

        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);

        assertThrows(RuntimeException.class, () -> invokeGetColumns(task, "my_table"));
    }

    @Test
    void testIgnoreColumnsAreFiltered() throws Throwable {
        var task = setupTask("MY_SCHEMA");
        var mockConnection = getField(task, "connection", Connection.class);
        var mockStatement = mock(Statement.class);
        var mockResultSet = buildResultSet(List.of("COL1", "COL2", "COL3"));

        Field ignoreColumnsField = SnowflakeSinkTask.class.getDeclaredField("ignoreColumns");
        ignoreColumnsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        var ignoreColumns = (List<String>) ignoreColumnsField.get(task);
        ignoreColumns.add("COL2");

        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);

        var result = invokeGetColumns(task, "my_table");

        assertEquals(List.of("COL1", "COL3"), result);
    }

    private SnowflakeSinkTask setupTask(String schemaName) throws Exception {
        var task = new SnowflakeSinkTask();

        Field connectionField = SnowflakeSinkTask.class.getDeclaredField("connection");
        connectionField.setAccessible(true);
        connectionField.set(task, mock(Connection.class));

        Field schemaNameField = SnowflakeSinkTask.class.getDeclaredField("schemaName");
        schemaNameField.setAccessible(true);
        schemaNameField.set(task, schemaName);

        return task;
    }

    @SuppressWarnings("unchecked")
    private <T> T getField(Object obj, String name, Class<T> type) throws Exception {
        var field = obj.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return (T) field.get(obj);
    }

    @SuppressWarnings("unchecked")
    private List<String> invokeGetColumns(SnowflakeSinkTask task, String table) throws Throwable {
        Method method = SnowflakeSinkTask.class.getDeclaredMethod("getColumnsFromMetadataInformationSchema", String.class);
        method.setAccessible(true);
        try {
            return (List<String>) method.invoke(task, table);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
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
