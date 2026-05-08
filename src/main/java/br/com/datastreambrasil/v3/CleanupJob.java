package br.com.datastreambrasil.v3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import java.sql.Connection;
import java.sql.SQLException;

public class CleanupJob implements Job {

    private static final Logger LOGGER = LogManager.getLogger(CleanupJob.class);

    protected static final String INGEST_TABLE_NAME = "ingest_table_name";
    protected static final String SNOWFLAKE_CONNECTION = "snowflake_connection";

    @Override
    public void execute(JobExecutionContext context) {
        var jobData = context.getMergedJobDataMap();
        var ingest = (String) jobData.get(INGEST_TABLE_NAME);
        var connection = (Connection) jobData.get(SNOWFLAKE_CONNECTION);

        // we delete records older than 4 hours only
        var deleteQuery = String.format("""
                    delete from %s ingest where ih_datetime + interval '%s hour' < sysdate()
            """, ingest, 4);

        LOGGER.debug("Executing delete query: {}", deleteQuery);
        try (var stmt = connection.createStatement()) {
            stmt.executeLargeUpdate(deleteQuery);
        } catch (SQLException e) {
            LOGGER.error("Error while executing delete query", e);
        }

    }

}
