package br.com.datastreambrasil.v2;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class CleanupJob implements Job {

    private static final Logger LOGGER = LoggerFactory.getLogger(CleanupJob.class);

    @Override
    @SuppressWarnings("unchecked")
    public void execute(JobExecutionContext context) throws JobExecutionException {
        var jobData = context.getMergedJobDataMap();
        var pk = (List<String>) jobData.get(SnowflakeSinkConnector.CFG_PK);
        var table = (String) jobData.get(SnowflakeSinkConnector.CFG_TABLE_NAME);
        var connection = (Connection) jobData.get(SnowflakeSinkTask.KEY_SNOWFLAKE_CONNECTION);

        var deleteQuery = String.format("""
                        delete from %s ingest using (select max(ih_offset) as ih_offset,
                         %s from %s
                         group by %s) as ingest_max where ingest.ih_offset < ingest_max.ih_offset and %s
                """, table, String.join(",", pk), table,
                String.join(",", pk), buildPkWhereClause(pk));

        LOGGER.debug("Executing delete query: {}", deleteQuery);
        try (var stmt = connection.createStatement()) {
            stmt.executeLargeUpdate(deleteQuery);
        } catch (SQLException e) {
            LOGGER.error("Error while executing delete query", e);
        }

    }

    private String buildPkWhereClause(List<String> pk) {
        return pk.stream()
                .map(col -> String.format("ingest.%s = ingest_max.%s", col, col))
                .reduce((a, b) -> a + " and " + b).orElseThrow();
    }

}
