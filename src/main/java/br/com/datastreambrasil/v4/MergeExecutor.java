package br.com.datastreambrasil.v4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Applies a range of the ingest table to the final table, used only when {@code ingestion_only}
 * is false.
 *
 * <p>The rows to apply are named by a half-open Kafka offset range per partition rather than by a
 * block id. The lower bound is the last offset already merged, which Kafka Connect persists as the
 * committed offset, so the range survives a restart: whatever a crash left unmerged in the ingest
 * table is still inside the next range and gets applied. A block id lives only in the task's
 * memory and would strand those rows.
 *
 * <p>v3 could rely on its in-memory buffer being keyed by primary key, so a batch never held two
 * rows for the same key. v4 streams every event, so the range must be deduplicated in SQL: the
 * {@code QUALIFY ROW_NUMBER()} runs <em>before</em> the {@code ih_op} filter, which both avoids
 * Snowflake's "Duplicate row detected during DML action" error and stops an update from
 * resurrecting a key whose last operation in the range was a delete.
 *
 * <p>The ordering assumes a given primary key always lands in the same Kafka partition, which
 * holds for Debezium topics keyed by primary key.
 */
public class MergeExecutor {

    private static final Logger LOGGER = LogManager.getLogger(MergeExecutor.class);

    /** One partition's slice of the range: everything after {@code from}, up to and including {@code to}. */
    public record OffsetRange(int partition, long fromExclusive, long toInclusive) {
        boolean isEmpty() {
            return toInclusive <= fromExclusive;
        }
    }

    private final Connection connection;
    private final String tableName;
    private final String ingestTableName;
    private final List<String> columnsFinalTable;
    private final List<String> ingestOnlyColumns;

    public MergeExecutor(Connection connection, String tableName, String ingestTableName,
                         List<String> columnsFinalTable, List<String> ingestOnlyColumns) {
        this.connection = connection;
        this.tableName = tableName;
        this.ingestTableName = ingestTableName;
        this.columnsFinalTable = List.copyOf(columnsFinalTable);
        this.ingestOnlyColumns = List.copyOf(ingestOnlyColumns);
    }

    public void merge(Map<Integer, OffsetRange> ranges, List<String> pks) {
        execute(buildMergeSql(ranges, pks), "merge");
    }

    public void deleteRows(Map<Integer, OffsetRange> ranges, List<String> pks) {
        execute(buildDeleteSql(ranges, pks), "delete");
    }

    private void execute(String sql, String label) {
        LOGGER.debug("Executing {} statement: {}", label, sql);
        var startTime = System.currentTimeMillis();
        try (var stmt = connection.createStatement()) {
            var affected = stmt.executeLargeUpdate(sql);
            LOGGER.info("{} affected {} rows in {} ms", label, affected,
                    System.currentTimeMillis() - startTime);
        } catch (SQLException e) {
            LOGGER.error("Error executing {} statement", label, e);
            throw new RuntimeException("Error executing " + label + " statement", e);
        }
    }

    protected String buildMergeSql(Map<Integer, OffsetRange> ranges, List<String> pks) {
        return String.format(
                "MERGE INTO %s AS final USING ("
                        + "SELECT %s FROM (%s) WHERE ih_op in ('c', 'r', 'u')"
                        + ") AS ingest ON %s "
                        + "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s) "
                        + "WHEN MATCHED THEN UPDATE SET %s",
                tableName,
                buildExcludeColumns(),
                latestPerPk(ranges, pks),
                buildPkWhereClause(pks),
                String.join(",", columnsFinalTable),
                String.join(",", columnsFinalTable.stream().map(c -> "ingest." + c).toList()),
                buildUpdateColumns());
    }

    protected String buildDeleteSql(Map<Integer, OffsetRange> ranges, List<String> pks) {
        return String.format(
                "DELETE FROM %s as final USING ("
                        + "SELECT %s FROM (%s) WHERE ih_op = 'd'"
                        + ") AS ingest WHERE %s",
                tableName,
                String.join(",", pks),
                latestPerPk(ranges, pks),
                buildPkWhereClause(pks));
    }

    private String latestPerPk(Map<Integer, OffsetRange> ranges, List<String> pks) {
        return String.format(
                "SELECT * FROM %s WHERE %s "
                        + "QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY ih_offset DESC) = 1",
                ingestTableName, buildRangePredicate(ranges), String.join(",", pks));
    }

    /**
     * Restricts the scan to the offsets that have not been merged yet, so the statement never
     * reprocesses the whole ingest table.
     */
    protected String buildRangePredicate(Map<Integer, OffsetRange> ranges) {
        var clauses = ranges.values().stream()
                .filter(r -> !r.isEmpty())
                .map(r -> String.format("(ih_partition = %d and ih_offset > %d and ih_offset <= %d)",
                        r.partition(), r.fromExclusive(), r.toInclusive()))
                .toList();

        if (clauses.isEmpty()) {
            throw new IllegalArgumentException("No non-empty offset range to merge");
        }
        return "(" + String.join(" or ", clauses) + ")";
    }

    protected String buildUpdateColumns() {
        return String.join(",", columnsFinalTable.stream()
                .map(column -> String.format("final.%s = ingest.%s", column, column))
                .toList());
    }

    /**
     * {@code *}, minus the ingest-only columns. Snowflake rejects an empty {@code EXCLUDE ()}, so
     * an ingest table whose columns all exist in the final table selects a plain {@code *}.
     */
    protected String buildExcludeColumns() {
        return ingestOnlyColumns.isEmpty()
                ? "*"
                : String.format("* EXCLUDE (%s)", String.join(",", ingestOnlyColumns));
    }

    protected String buildPkWhereClause(List<String> pks) {
        return pks.stream()
                .map(col -> String.format("final.%s = ingest.%s", col, col))
                .reduce((a, b) -> String.format("%s and %s", a, b))
                .orElseThrow(() -> new IllegalStateException("No primary key columns to join on"));
    }
}
