package br.com.datastreambrasil.v3;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CleanupJobTest {

    private JobExecutionContext contextWith(Object ingestTables, Connection connection) {
        var context = mock(JobExecutionContext.class);
        var jobDataMap = new JobDataMap();
        if (ingestTables != null) {
            jobDataMap.put(CleanupJob.INGEST_TABLE_NAMES, ingestTables);
        }
        if (connection != null) {
            jobDataMap.put(CleanupJob.SNOWFLAKE_CONNECTION, connection);
        }
        when(context.getMergedJobDataMap()).thenReturn(jobDataMap);
        return context;
    }

    @Test
    void execute_withNullIngestTables_skipsCleanup() throws SQLException {
        var connection = mock(Connection.class);
        var context = contextWith(null, connection);

        new CleanupJob().execute(context);

        verify(connection, never()).createStatement();
    }

    @Test
    void execute_withEmptyIngestTables_skipsCleanup() throws SQLException {
        var connection = mock(Connection.class);
        var context = contextWith(Set.of(), connection);

        new CleanupJob().execute(context);

        verify(connection, never()).createStatement();
    }

    @Test
    void execute_withIngestTable_executesDeleteQuery() throws SQLException {
        var connection = mock(Connection.class);
        var stmt = mock(Statement.class);
        when(connection.createStatement()).thenReturn(stmt);
        var context = contextWith(Set.of("MY_TABLE_INGEST"), connection);

        new CleanupJob().execute(context);

        var captor = ArgumentCaptor.forClass(String.class);
        verify(stmt, times(1)).executeLargeUpdate(captor.capture());
        var sql = captor.getValue();
        assertTrue(sql.contains("MY_TABLE_INGEST"), "DELETE must reference the ingest table name");
        assertTrue(sql.toLowerCase().contains("delete"), "SQL must be a DELETE statement");
        assertTrue(sql.contains("ih_datetime"), "DELETE must filter by ih_datetime");
    }

    @Test
    void execute_withMultipleIngestTables_executesDeleteForEachTable() throws SQLException {
        var connection = mock(Connection.class);
        var stmt = mock(Statement.class);
        when(connection.createStatement()).thenReturn(stmt);
        var context = contextWith(Set.of("TABLE_A_INGEST", "TABLE_B_INGEST"), connection);

        new CleanupJob().execute(context);

        verify(connection, times(2)).createStatement();
        verify(stmt, times(2)).executeLargeUpdate(any());
    }

    @Test
    void execute_whenSqlExceptionThrown_logsErrorAndDoesNotRethrow() throws SQLException {
        var connection = mock(Connection.class);
        var stmt = mock(Statement.class);
        when(connection.createStatement()).thenReturn(stmt);
        when(stmt.executeLargeUpdate(any())).thenThrow(new SQLException("db error"));
        var context = contextWith(Set.of("MY_TABLE_INGEST"), connection);

        assertDoesNotThrow(() -> new CleanupJob().execute(context),
                "SQLException must be caught internally and not propagate");
    }
}
