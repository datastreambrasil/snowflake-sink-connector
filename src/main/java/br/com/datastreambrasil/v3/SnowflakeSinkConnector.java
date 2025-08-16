package br.com.datastreambrasil.v3;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SnowflakeSinkConnector extends SinkConnector {

    protected static final String VERSION = "v3";
    protected static final String CFG_STAGE_NAME = "stage";
    protected static final String CFG_TABLE_NAME = "table";
    protected static final String CFG_TIMESTAMP_FIELDS_CONVERT = "timestamp_fields_convert";
    protected static final String CFG_DATE_FIELDS_CONVERT = "date_fields_convert";
    protected static final String CFG_TIME_FIELDS_CONVERT = "time_fields_convert";
    protected static final String CFG_SCHEMA_NAME = "schema";
    protected static final String CFG_URL = "url";
    protected static final String CFG_USER = "user";
    protected static final String CFG_PASSWORD = "password";
    protected static final String CFG_PK = "pk";
    protected static final String CFG_JOB_CLEANUP_DURATION = "job_cleanup_duration";
    protected static final String CFG_JOB_CLEANUP_DISABLE = "job_cleanup_disable";
    protected static final String CFG_IGNORE_COLUMNS = "ignore_columns";
    protected static final String CFG_REDIS_HOST = "redis_host";
    protected static final String CFG_REDIS_PORT = "redis_port";
    protected static final String CFG_REDIS_KEY_TTL_SECONDS = "redis_key_ttl_seconds";
    protected static final String CFG_PROFILE = "profile";
    protected static final String CFG_HASHING_SUPPORT = "hasing_support";

    /*
     * For some use cases we need to load all data again, each time. So we have two
     * parameters to allow this:
     * always_truncate_before_bulk: If true, we will truncate the table before
     * copying it to snowflake.
     * truncate_when_nodata_after_seconds: If we don't receive any event for this
     * amount of time, we will truncate the table in snowflake.
     */
    protected static final String CFG_ALWAYS_TRUNCATE_BEFORE_BULK = "always_truncate_before_bulk";
    protected static final String CFG_TRUNCATE_WHEN_NODATA_AFTER_SECONDS = "truncate_when_nodata_after_seconds";

    static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(CFG_STAGE_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
            "Stage name to write files")
        .define(CFG_URL, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
            "URL to snowflake connection")
        .define(CFG_USER, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
            "User to snowflake connection")
        .define(CFG_PASSWORD, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
            "Password to snowflake connection")
        .define(CFG_TABLE_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
            "Target table to copy data into")
        .define(CFG_TIMESTAMP_FIELDS_CONVERT, ConfigDef.Type.LIST, Collections.emptyList(),
            ConfigDef.Importance.MEDIUM,
            "List of timestamp fields we should convert to LocalDateTime")
        .define(CFG_DATE_FIELDS_CONVERT, ConfigDef.Type.LIST, Collections.emptyList(),
            ConfigDef.Importance.MEDIUM,
            "List of date fields we should convert to LocalDate")
        .define(CFG_TIME_FIELDS_CONVERT, ConfigDef.Type.LIST, Collections.emptyList(),
            ConfigDef.Importance.MEDIUM,
            "List of time fields we should convert to LocalTime")
        .define(CFG_PK, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.MEDIUM,
            "List of primary keys to be used.")
        .define(CFG_IGNORE_COLUMNS, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.MEDIUM,
            "List of columns to ignore")
        .define(CFG_JOB_CLEANUP_DURATION, ConfigDef.Type.STRING, "PT4H", ConfigDef.Importance.MEDIUM,
            "Uses duration parse format, as defined here (https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html#parse(java.lang.CharSequence)")
        .define(CFG_SCHEMA_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
            "Target schema to copy data into")
        .define(CFG_JOB_CLEANUP_DISABLE, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.HIGH,
            "Disable clean up job")
        .define(CFG_ALWAYS_TRUNCATE_BEFORE_BULK, ConfigDef.Type.BOOLEAN, false,
            ConfigDef.Importance.HIGH,
            "If true, we will truncate the table before copying it to snowflake")
        .define(CFG_REDIS_HOST, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
            "redis host")
        .define(CFG_REDIS_PORT, ConfigDef.Type.INT, null, ConfigDef.Importance.HIGH,
            "redis port")
        .define(CFG_REDIS_KEY_TTL_SECONDS, ConfigDef.Type.INT, 120, ConfigDef.Importance.HIGH,
            "redis key ttl in seconds.")
        .define(CFG_PROFILE, ConfigDef.Type.STRING, "cdc_schema",
            ConfigDef.Importance.HIGH,
            "Profile to use for the connector. Might be one of: cdc_schema, cdc_schemaless, bulk_schemaless")
        .define(CFG_HASHING_SUPPORT, ConfigDef.Type.BOOLEAN, false,
            ConfigDef.Importance.MEDIUM,
            "When true, we will calculate hash for before and after struct and use it if PK does not exist. This is a sort of surrogate key for the record.")
        .define(CFG_TRUNCATE_WHEN_NODATA_AFTER_SECONDS, ConfigDef.Type.INT, 1800,
            ConfigDef.Importance.HIGH,
            "If we don't receive any event for this amount of time, we will truncate the table in snowflake");

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
            var propsTask = new java.util.HashMap<>(props);
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
