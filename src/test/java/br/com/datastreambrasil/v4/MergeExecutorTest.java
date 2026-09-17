package br.com.datastreambrasil.v4;

import br.com.datastreambrasil.v4.MergeExecutor.OffsetRange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MergeExecutorTest {

    private static final List<String> FINAL_COLUMNS = List.of("ID", "NAME");
    private static final List<String> INGEST_ONLY_COLUMNS =
            List.of("IH_TOPIC", "IH_PARTITION", "IH_OFFSET", "IH_OP", "IH_DATETIME", "IH_BLOCKID");

    /** One partition, offsets 100 (exclusive) through 500 (inclusive). */
    private static final Map<Integer, OffsetRange> RANGE = Map.of(0, new OffsetRange(0, 100, 500));

    private Connection connection;
    private Statement statement;
    private MergeExecutor executor;

    @BeforeEach
    void setUp() throws SQLException {
        connection = mock(Connection.class);
        statement = mock(Statement.class);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeLargeUpdate(anyString())).thenReturn(1L);

        executor = new MergeExecutor(connection, "EVENTS", "EVENTS_INGEST",
                FINAL_COLUMNS, INGEST_ONLY_COLUMNS);
    }

    @Test
    void mergeDeduplicatesTheRangeBeforeFilteringByOperation() {
        var sql = executor.buildMergeSql(RANGE, List.of("ID"));

        // v4 streams every event, so a range can hold several rows per key. Without this the
        // MERGE fails with "Duplicate row detected during DML action".
        assertTrue(sql.contains("QUALIFY ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ih_offset DESC) = 1"),
                sql);

        // the op filter must come after the dedup, otherwise an update in the same range would
        // resurrect a key whose last operation was a delete
        var qualifyAt = sql.indexOf("QUALIFY ROW_NUMBER()");
        var opFilterAt = sql.indexOf("WHERE ih_op in ('c', 'r', 'u')");
        assertTrue(qualifyAt > 0 && opFilterAt > qualifyAt,
                "ih_op filter must be applied after the dedup: " + sql);
    }

    @Test
    void selectsByOffsetRangeSoARestartCannotStrandRows() {
        var sql = executor.buildMergeSql(RANGE, List.of("ID"));

        // the lower bound is the last merged offset, which Connect persists, rather than a block
        // id that only exists in the task's memory
        assertTrue(sql.contains("(ih_partition = 0 and ih_offset > 100 and ih_offset <= 500)"), sql);
        assertTrue(sql.contains("EVENTS_INGEST"), sql);
    }

    @Test
    void coversEveryPartitionInOneStatement() {
        var ranges = Map.of(
                0, new OffsetRange(0, 100, 500),
                1, new OffsetRange(1, 7, 9));

        var predicate = executor.buildRangePredicate(ranges);

        assertTrue(predicate.contains("ih_partition = 0 and ih_offset > 100 and ih_offset <= 500"), predicate);
        assertTrue(predicate.contains("ih_partition = 1 and ih_offset > 7 and ih_offset <= 9"), predicate);
        assertTrue(predicate.contains(" or "), predicate);
    }

    @Test
    void skipsPartitionsWithNothingNew() {
        var ranges = Map.of(
                0, new OffsetRange(0, 100, 500),
                1, new OffsetRange(1, 42, 42));   // nada novo nesta particao

        var predicate = executor.buildRangePredicate(ranges);

        assertTrue(predicate.contains("ih_partition = 0"), predicate);
        assertTrue(!predicate.contains("ih_partition = 1"), predicate);
    }

    @Test
    void refusesToBuildAStatementWithNothingToMerge() {
        var empty = Map.of(0, new OffsetRange(0, 500, 500));

        assertThrows(IllegalArgumentException.class, () -> executor.buildRangePredicate(empty));
    }

    @Test
    void mergeExcludesIngestOnlyColumnsAndUpdatesEveryFinalColumn() {
        var sql = executor.buildMergeSql(RANGE, List.of("ID"));

        assertTrue(sql.contains("EXCLUDE (IH_TOPIC,IH_PARTITION,IH_OFFSET,IH_OP,IH_DATETIME,IH_BLOCKID)"), sql);
        assertTrue(sql.contains("MERGE INTO EVENTS AS final"), sql);
        assertTrue(sql.contains("WHEN NOT MATCHED THEN INSERT (ID,NAME) VALUES (ingest.ID,ingest.NAME)"), sql);
        assertTrue(sql.contains("WHEN MATCHED THEN UPDATE SET final.ID = ingest.ID,final.NAME = ingest.NAME"), sql);
    }

    @Test
    void withNothingToExcludeItSelectsAPlainStar() {
        // Snowflake rejects EXCLUDE (), so an ingest table whose columns all exist in the final
        // table has to select * instead
        var noExtras = new MergeExecutor(mock(Connection.class), "EVENTS", "EVENTS_INGEST",
                List.of("ID", "NAME"), List.of());

        var sql = noExtras.buildMergeSql(RANGE, List.of("ID"));

        assertTrue(sql.contains("SELECT * FROM"), sql);
        assertTrue(!sql.contains("EXCLUDE"), sql);
    }

    @Test
    void deleteDeduplicatesTheRangeBeforeFilteringByOperation() {
        var sql = executor.buildDeleteSql(RANGE, List.of("ID"));

        var qualifyAt = sql.indexOf("QUALIFY ROW_NUMBER()");
        var opFilterAt = sql.indexOf("WHERE ih_op = 'd'");
        assertTrue(qualifyAt > 0 && opFilterAt > qualifyAt, sql);
        assertTrue(sql.contains("DELETE FROM EVENTS as final"), sql);
    }

    @Test
    void supportsCompositePrimaryKeys() {
        var sql = executor.buildMergeSql(RANGE, List.of("ID", "TENANT"));

        assertTrue(sql.contains("PARTITION BY ID,TENANT ORDER BY ih_offset DESC"), sql);
        assertTrue(sql.contains("final.ID = ingest.ID and final.TENANT = ingest.TENANT"), sql);
    }

    @Test
    void executesTheStatements() throws SQLException {
        executor.merge(RANGE, List.of("ID"));
        executor.deleteRows(RANGE, List.of("ID"));

        verify(statement).executeLargeUpdate(matches("(?s)MERGE INTO EVENTS.*"));
        verify(statement).executeLargeUpdate(matches("(?s)DELETE FROM EVENTS.*"));
        verify(connection, times(2)).createStatement();
    }
}
