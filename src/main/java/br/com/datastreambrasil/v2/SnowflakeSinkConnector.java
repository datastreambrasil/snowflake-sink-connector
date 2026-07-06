package br.com.datastreambrasil.v2;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SnowflakeSinkConnector extends SinkConnector {

    protected static final String VERSION = "v2";
    protected static final String CFG_STAGE_NAME = "stage";
    protected static final String CFG_TABLE_NAME = "table";
    protected static final String CFG_TIMESTAMP_FIELDS_CONVERT_SECONDS = "timestamp_fields_convert_seconds";
    protected static final String CFG_DATE_FIELDS_CONVERT = "date_fields_convert";
    protected static final String CFG_TIME_FIELDS_CONVERT = "time_fields_convert";
    protected static final String CFG_SCHEMA_NAME = "schema";
    protected static final String CFG_URL = "url";
    protected static final String CFG_USER = "user";
    protected static final String CFG_PASSWORD = "password";
    protected static final String CFG_PK = "pk";
    protected static final String CFG_JOB_CLEANUP_HOURS = "job_cleanup_hours";
    protected static final String CFG_JOB_CLEANUP_DISABLE = "job_cleanup_disable";
    protected static final String CFG_PAYLOAD_CDC_FORMAT = "payload_cdc_format";
    protected static final String CFG_IGNORE_COLUMNS = "ignore_columns";
    protected static final String FIND_COLUMNS_IN_METADATA = "find_columns_in_metadata";
    protected static final String CFG_TABLE_FIELDS = "table_fields";

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

    protected static final String CFG_REMOVE_STAGE_AFTER_COPY = "remove_stage_after_copy";

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
            .define(CFG_TIMESTAMP_FIELDS_CONVERT_SECONDS, ConfigDef.Type.LIST, null,
                    ConfigDef.Importance.MEDIUM,
                    "List of timestamp fields we should convert to seconds")
            .define(CFG_DATE_FIELDS_CONVERT, ConfigDef.Type.LIST, null,
                    ConfigDef.Importance.MEDIUM,
                    "List of date fields we should convert to LocalDate")
            .define(CFG_TIME_FIELDS_CONVERT, ConfigDef.Type.LIST, null,
                    ConfigDef.Importance.MEDIUM,
                    "List of time fields we should convert to LocalTime")
            .define(CFG_IGNORE_COLUMNS, ConfigDef.Type.LIST, null, ConfigDef.Importance.MEDIUM,
                    "List of columns to ignore")
            .define(CFG_PK, ConfigDef.Type.LIST, null, ConfigDef.Importance.MEDIUM,
                    "List of primary keys to be used")
            .define(CFG_JOB_CLEANUP_HOURS, ConfigDef.Type.INT, 4, ConfigDef.Importance.MEDIUM,
                    "Interval in hours to cleanup old data")
            .define(CFG_SCHEMA_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                    "Target schema to copy data into")
            .define(CFG_JOB_CLEANUP_DISABLE, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH,
                    "Disable clean up job")
            .define(CFG_PAYLOAD_CDC_FORMAT, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH,
                    "Set if payload is CDC format")
            .define(CFG_ALWAYS_TRUNCATE_BEFORE_BULK, ConfigDef.Type.BOOLEAN, false,
                    ConfigDef.Importance.HIGH,
                    "If true, we will truncate the table before copying it to snowflake")
            .define(CFG_REMOVE_STAGE_AFTER_COPY, ConfigDef.Type.BOOLEAN, true,
                    ConfigDef.Importance.HIGH,
                    "If true, we will remove the stage after copying it to snowflake")
            .define(CFG_TRUNCATE_WHEN_NODATA_AFTER_SECONDS, ConfigDef.Type.INT, 1800,
                    ConfigDef.Importance.HIGH,
                    "If we don't receive any event for this amount of time, we will truncate the table in snowflake")
            .define(FIND_COLUMNS_IN_METADATA, ConfigDef.Type.BOOLEAN, Boolean.FALSE,
                    ConfigDef.Importance.HIGH,
                    "Define whether to retrieve column names from the metadata or by querying the information schema.")
            .define(CFG_TABLE_FIELDS, ConfigDef.Type.LIST, List.of(),
                    ConfigDef.Importance.HIGH,
                    "Defines the names of the fields of table to be processed.");

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
            configs.add(props);
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
