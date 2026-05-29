package br.com.datastreambrasil.v3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

public class CleanupJob implements Job {

    private static final Logger LOGGER = LogManager.getLogger(CleanupJob.class);

    protected static final String INGEST_TABLE_NAMES = "ingest_table_names";
    protected static final String SNOWFLAKE_CONNECTION = "snowflake_connection";

    @Override
    public void execute(JobExecutionContext context) {
        var jobData = context.getMergedJobDataMap();
        @SuppressWarnings("unchecked")
        var ingestTables = (Set<String>) jobData.get(INGEST_TABLE_NAMES);
        var connection = (Connection) jobData.get(SNOWFLAKE_CONNECTION);

        if (ingestTables == null || ingestTables.isEmpty()) {
            LOGGER.debug("No ingest tables registered yet, skipping cleanup.");
            return;
        }

        for (var ingest : ingestTables) {
            var deleteQuery = String.format("""
                        delete from %s ingest where ih_datetime + interval '%s hour' < sysdate()
                """, ingest, 4);

            LOGGER.debug("Executing delete query: {}", deleteQuery);
            try (var stmt = connection.createStatement()) {
                stmt.executeLargeUpdate(deleteQuery);
            } catch (SQLException e) {
                LOGGER.error("Error while executing delete query for table {}", ingest, e);
            }
        }
    }
}
