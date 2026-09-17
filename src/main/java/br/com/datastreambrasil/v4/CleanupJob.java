package br.com.datastreambrasil.v4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Trims the ingest table once a block has been merged into the final table.
 *
 * <p>Only scheduled when {@code ingestion_only} is false. With {@code ingestion_only=true} the
 * ingest table <em>is</em> the source of truth for whatever downstream object deduplicates it
 * (a Dynamic Table, typically), and deleting rows here would delete current state.
 */
public class CleanupJob implements Job {

    private static final Logger LOGGER = LogManager.getLogger(CleanupJob.class);

    protected static final String INGEST_TABLE_NAME = "ingest_table_name";
    protected static final String SNOWFLAKE_CONNECTION = "snowflake_connection";
    protected static final String RETENTION_HOURS = "retention_hours";

    @Override
    public void execute(JobExecutionContext context) {
        var jobData = context.getMergedJobDataMap();
        var ingest = (String) jobData.get(INGEST_TABLE_NAME);
        var connection = (Connection) jobData.get(SNOWFLAKE_CONNECTION);
        var retentionHours = jobData.get(RETENTION_HOURS);

        var deleteQuery = String.format(
                "delete from %s ingest where ih_datetime + interval '%s hour' < sysdate()",
                ingest, retentionHours);

        LOGGER.debug("Executing delete query: {}", deleteQuery);
        try (var stmt = connection.createStatement()) {
            stmt.executeLargeUpdate(deleteQuery);
        } catch (SQLException e) {
            LOGGER.error("Error while executing delete query", e);
        }
    }
}
