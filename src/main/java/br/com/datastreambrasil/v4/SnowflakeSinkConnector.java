package br.com.datastreambrasil.v4;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * v4 writes straight into the {@code <TABLE>_INGEST} table through Snowpipe Streaming
 * (high-performance architecture), dropping the PUT-to-stage and COPY INTO steps of v3.
 *
 * <p>Two modes:
 * <ul>
 *   <li>{@code ingestion_only=true} — rows land in {@code _INGEST} and nothing else happens.
 *       Deduplication is expected to be done in Snowflake (a Dynamic Table, for instance).
 *       No warehouse is used at steady state and the {@code _INGEST} cleanup job never runs.</li>
 *   <li>{@code ingestion_only=false} — after every commit cycle a MERGE/DELETE moves the block
 *       into the final table, like v3 did.</li>
 * </ul>
 *
 * <p>Two record formats: a Debezium envelope carrying a schema, and a flat JSON row with the
 * operation in a header. Which one a record is gets detected per record, so neither needs
 * configuring; {@code profile} pins it to one when that is preferable. Both produce the same
 * ingest rows, so everything downstream is unchanged.
 */
public class SnowflakeSinkConnector extends SinkConnector {

    protected static final String VERSION = "v4";

    // snowflake identity
    protected static final String CFG_SCHEMA_NAME = "schema";
    protected static final String CFG_TABLE_NAME = "table";
    protected static final String CFG_USER = "user";
    /** Same key v2/v3 use: the value is the programmatic access token (PAT). */
    protected static final String CFG_PASSWORD = "password";
    protected static final String CFG_ROLE = "role";
    protected static final String CFG_STREAMING_URL = "streaming_url";
    protected static final String CFG_URL = "url";

    // streaming
    protected static final String CFG_PIPE = "pipe";
    protected static final String CFG_CHANNEL_NAME_PREFIX = "channel_name_prefix";
    protected static final String CFG_MAX_CLIENT_LAG_SECONDS = "max_client_lag_seconds";
    protected static final String CFG_MERGE_INTERVAL = "merge_interval";
    protected static final String CFG_APPEND_MAX_RETRIES = "append_max_retries";
    protected static final String CFG_FAIL_ON_ROW_ERROR = "fail_on_row_error";

    // behaviour
    protected static final String CFG_INGESTION_ONLY = "ingestion_only";
    protected static final String CFG_PROFILE = "profile";
    protected static final String CFG_OP_HEADER = "op_header";
    protected static final String CFG_PK_FIELDS = "pk_fields";

    // carried over from v3
    protected static final String CFG_TIMESTAMP_FIELDS_CONVERT = "timestamp_fields_convert";
    protected static final String CFG_DATE_FIELDS_CONVERT = "date_fields_convert";
    protected static final String CFG_TIME_FIELDS_CONVERT = "time_fields_convert";
    protected static final String CFG_IGNORE_COLUMNS = "ignore_columns";
    protected static final String CFG_JOB_CLEANUP_DURATION = "job_cleanup_duration";
    protected static final String CFG_JOB_CLEANUP_DISABLE = "job_cleanup_disable";
    protected static final String CFG_JOB_CLEANUP_RETENTION_HOURS = "job_cleanup_retention_hours";
    protected static final String CFG_FIND_COLUMNS_IN_METADATA = "find_columns_in_metadata";
    protected static final String CFG_EXCLUDE_INGEST_ADDITIONAL_FIELDS = "exclude_ingest_additional_fields";
    protected static final String CFG_CONSUMER_OVERRIDE_MAX_POLL_RECORDS = "consumer.override.max.poll.records";
    protected static final String CFG_CONSUMER_OVERRIDE_MAX_POLL_INTERVAL_MS = "consumer.override.max.poll.interval.ms";

    protected static final String PROFILE_AUTO = "auto";
    protected static final String PROFILE_CDC_SCHEMA = "cdc_schema";
    protected static final String PROFILE_FLAT_JSON = "flat_json";

    static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(CFG_SCHEMA_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
            "Target schema. Falls back to the 'schema' parameter of the JDBC URL.")
        .define(CFG_TABLE_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
            "Final table name. Rows are streamed into <table>_INGEST.")
        .define(CFG_USER, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
            "Snowflake user owning the token")
        .define(CFG_PASSWORD, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.HIGH,
            "Snowflake programmatic access token (PAT), passed as the password like v2/v3 do. "
                + "The streaming SDK gets the same value as authorization_type=PAT.")
        .define(CFG_ROLE, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
            "Optional role for the streaming session. Unset by default, so the user's default role "
                + "applies, as in v3. Only needed when the PAT's ROLE_RESTRICTION names a different "
                + "role. Never sent over JDBC.")
        .define(CFG_STREAMING_URL, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
            "Streaming SDK endpoint. Defaults to https://<host of url>:443, so PrivateLink and "
                + "region-qualified hosts are already handled. Only set it when the streaming "
                + "endpoint differs from the JDBC one.")
        .define(CFG_URL, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
            "JDBC URL, used for column discovery and - when ingestion_only is false - for MERGE/DELETE")

        .define(CFG_PIPE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
            "Pipe to stream into. Defaults to the auto-created default pipe '<table>_INGEST-STREAMING'.")
        .define(CFG_CHANNEL_NAME_PREFIX, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
            "Prefix for channel names. Defaults to the connector name. Channels are named "
                + "<prefix>_<topic>_<partition> and must be stable across restarts.")
        .define(CFG_MAX_CLIENT_LAG_SECONDS, ConfigDef.Type.INT, null, ConfigDef.Importance.MEDIUM,
            "SDK parameter override 'max_client_lag_seconds'. Higher values buffer longer and produce "
                + "fewer, larger files. Leave unset to use the SDK default.")
        .define(CFG_MERGE_INTERVAL, ConfigDef.Type.STRING, "PT0S", ConfigDef.Importance.HIGH,
            "Minimum time between MERGE/DELETE cycles, in Duration format. Only used when "
                + "ingestion_only is false. The default of PT0S merges on every Connect commit "
                + "(offset.flush.interval.ms, 60s by default), which costs a warehouse resume per "
                + "cycle regardless of how few rows arrived - the warehouse bills a 60s minimum "
                + "each time. Raising this to PT15M cuts the cycles by 15x; rows simply wait in "
                + "the ingest table, and Kafka offsets wait with them, so nothing is lost.")
        .define(CFG_APPEND_MAX_RETRIES, ConfigDef.Type.INT, 5, ConfigDef.Importance.MEDIUM,
            "Retries with exponential backoff for retryable appendRow failures (429/500/503)")
        .define(CFG_FAIL_ON_ROW_ERROR, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH,
            "Fail the task when Snowflake reports rows rejected by the pipe. Defaults to true to "
                + "match v3, whose COPY aborted the statement on a bad row. The streaming pipe only "
                + "supports ON_ERROR=CONTINUE, so with this off a rejected row is silently dropped "
                + "and only visible in the error table.")

        .define(CFG_INGESTION_ONLY, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.HIGH,
            "If true, only stream into <table>_INGEST: no MERGE, no DELETE and no cleanup job. "
                + "Deduplication is left to Snowflake (e.g. a Dynamic Table).")
        .define(CFG_PROFILE, ConfigDef.Type.STRING, PROFILE_AUTO, ConfigDef.Importance.MEDIUM,
            "Record format. 'auto' (the default) detects it per record and needs no configuration: "
                + "a Struct whose schema has 'op' next to 'before'/'after' is a Debezium envelope, "
                + "anything else is a flat JSON row with the operation in the '" + CFG_OP_HEADER
                + "' header. Set 'cdc_schema' or 'flat_json' to turn the detection off and reject "
                + "anything that is not that shape.")
        .define(CFG_OP_HEADER, ConfigDef.Type.STRING, "op", ConfigDef.Importance.MEDIUM,
            "Header carrying the operation (r, c, u or d) in the 'flat_json' profile. Matched "
                + "case-insensitively. Ignored by 'cdc_schema'.")
        .define(CFG_PK_FIELDS, ConfigDef.Type.LIST, Collections.emptyList(),
            ConfigDef.Importance.HIGH,
            "Primary key columns for the MERGE. Leave empty to take them from the record key, "
                + "which is what both profiles do by default. Required when merging a topic whose "
                + "records carry no key. Unused when ingestion_only is true.")

        .define(CFG_TIMESTAMP_FIELDS_CONVERT, ConfigDef.Type.LIST, Collections.emptyList(),
            ConfigDef.Importance.MEDIUM, "Columns whose epoch-millis value must be converted to LocalDateTime")
        .define(CFG_DATE_FIELDS_CONVERT, ConfigDef.Type.LIST, Collections.emptyList(),
            ConfigDef.Importance.MEDIUM, "Columns whose epoch-days value must be converted to LocalDate")
        .define(CFG_TIME_FIELDS_CONVERT, ConfigDef.Type.LIST, Collections.emptyList(),
            ConfigDef.Importance.MEDIUM, "Columns whose nanos-of-day value must be converted to LocalTime")
        .define(CFG_IGNORE_COLUMNS, ConfigDef.Type.LIST, Collections.emptyList(),
            ConfigDef.Importance.MEDIUM, "Columns of the ingest table to never write to")
        .define(CFG_JOB_CLEANUP_DURATION, ConfigDef.Type.STRING, "PT4H", ConfigDef.Importance.MEDIUM,
            "How often the ingest cleanup job runs. Duration format.")
        .define(CFG_JOB_CLEANUP_DISABLE, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.HIGH,
            "Disable the ingest cleanup job. Always disabled when ingestion_only is true.")
        .define(CFG_JOB_CLEANUP_RETENTION_HOURS, ConfigDef.Type.INT, 4, ConfigDef.Importance.MEDIUM,
            "How many hours of rows the cleanup job keeps in the ingest table")
        .define(CFG_FIND_COLUMNS_IN_METADATA, ConfigDef.Type.BOOLEAN, Boolean.FALSE,
            ConfigDef.Importance.HIGH,
            "Read column names from JDBC metadata (SHOW COLUMNS, no warehouse needed) instead of "
                + "querying INFORMATION_SCHEMA.")
        .define(CFG_EXCLUDE_INGEST_ADDITIONAL_FIELDS, ConfigDef.Type.LIST,
            List.of("IH_TOPIC", "IH_PARTITION", "IH_OFFSET", "IH_OP", "IH_DATETIME", "IH_BLOCKID",
                "IH_SCHEMA", "IH_TABLE"),
            ConfigDef.Importance.HIGH,
            "Ingest-table columns that do not exist in the final table. Names the ingest table does "
                + "not actually have are ignored, so the default can list columns like IH_BLOCKID, "
                + "IH_SCHEMA and IH_TABLE that only some tables carry. IH_SCHEMA and IH_TABLE are "
                + "filled from the headers of the same name, and only for a flat JSON record.")
        .define(CFG_CONSUMER_OVERRIDE_MAX_POLL_RECORDS, ConfigDef.Type.INT, 500,
            ConfigDef.Importance.MEDIUM,
            "Limits the records KafkaConsumer retrieves from the broker before they reach the connector.")
        .define(CFG_CONSUMER_OVERRIDE_MAX_POLL_INTERVAL_MS, ConfigDef.Type.STRING, "300000",
            ConfigDef.Importance.MEDIUM,
            "How long the broker waits for connector processing before triggering a rebalance.");

    private Map<String, String> props;

    @Override
    public void start(Map<String, String> map) {
        this.props = map;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SnowflakeSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            var propsTask = new HashMap<>(props);
            if (i > 0) {
                propsTask.put(CFG_JOB_CLEANUP_DISABLE, "true");
            }
            configs.add(propsTask);
        }
        return configs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return VERSION;
    }
}
