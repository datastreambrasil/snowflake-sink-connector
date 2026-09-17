package br.com.datastreambrasil.v4;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.SFException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class SnowflakeSinkTask extends SinkTask {

    private static final Logger LOGGER = LogManager.getLogger(SnowflakeSinkTask.class);

    protected static final String INGEST_SUFFIX = "_INGEST";

    private boolean ingestionOnly;
    private boolean failOnRowError;
    private String tableName;
    private String ingestTableName;
    private String databaseName;
    private String schemaName;
    private String account;
    private String streamingUrl;

    private SnowflakeJdbc jdbc;
    private StreamingChannelManager channels;
    private RowMapper rowMapper;
    private MergeExecutor mergeExecutor;
    private Scheduler scheduler;

    private List<String> pks = List.of();

    /** Offsets already durable in Snowflake when a partition was opened, used to drop replays. */
    private final Map<TopicPartition, Long> committedAtOpen = new HashMap<>();
    /** Rejected-row counters last reported per partition, to log only the delta. */
    private final Map<TopicPartition, Long> rowErrorCounts = new HashMap<>();

    // merge-mode bookkeeping, only touched from the single Connect worker thread.
    //
    // lastMerged is the watermark: everything at or below it has been applied to the final table,
    // and it is exactly what preCommit reports to Kafka. Seeding it from the first record seen on
    // a partition means a restart picks up wherever the previous run stopped merging, so a crash
    // between "rows are in _INGEST" and "rows are merged" costs nothing.
    private final Map<TopicPartition, Long> lastMerged = new HashMap<>();
    private final Map<TopicPartition, Long> appendedMax = new HashMap<>();
    private boolean blockHasUpserts;
    private boolean blockHasDeletes;
    private Instant lastMergeAt = Instant.EPOCH;
    private Duration mergeInterval = Duration.ZERO;

    /** Stamped into ih_blockid purely for tracing; the MERGE selects on the offset range. */
    private String currentBatchId = UUID.randomUUID().toString();

    @Override
    public String version() {
        return SnowflakeSinkConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> map) {
        var config = new AbstractConfig(SnowflakeSinkConnector.CONFIG_DEF, map);

        var profile = config.getString(SnowflakeSinkConnector.CFG_PROFILE);
        if (!SnowflakeSinkConnector.PROFILE_AUTO.equals(profile)
                && !SnowflakeSinkConnector.PROFILE_CDC_SCHEMA.equals(profile)
                && !SnowflakeSinkConnector.PROFILE_FLAT_JSON.equals(profile)) {
            throw new ConnectException("Unknown profile: " + profile + ". Valid values are "
                    + SnowflakeSinkConnector.PROFILE_AUTO + ", "
                    + SnowflakeSinkConnector.PROFILE_CDC_SCHEMA + " and "
                    + SnowflakeSinkConnector.PROFILE_FLAT_JSON);
        }

        ingestionOnly = config.getBoolean(SnowflakeSinkConnector.CFG_INGESTION_ONLY);
        failOnRowError = config.getBoolean(SnowflakeSinkConnector.CFG_FAIL_ON_ROW_ERROR);
        mergeInterval = Duration.parse(config.getString(SnowflakeSinkConnector.CFG_MERGE_INTERVAL));
        tableName = config.getString(SnowflakeSinkConnector.CFG_TABLE_NAME);
        ingestTableName = tableName + INGEST_SUFFIX;
        resolveSnowflakeLocation(config);

        jdbc = createJdbc(config);

        var columnsIngestTable = jdbc.columnsOf(ingestTableName,
                config.getBoolean(SnowflakeSinkConnector.CFG_FIND_COLUMNS_IN_METADATA),
                config.getList(SnowflakeSinkConnector.CFG_IGNORE_COLUMNS));

        var ingestOnlyColumns = excludedColumnsPresentIn(columnsIngestTable, config);
        var columnsFinalTable = columnsIngestTable.stream()
                .filter(c -> !ingestOnlyColumns.contains(c.toUpperCase(Locale.ROOT)))
                .toList();

        rowMapper = createRowMapper(profile, config, columnsIngestTable);

        channels = createChannelManager(config, map);

        if (ingestionOnly) {
            // nothing else needs Snowflake over JDBC: no MERGE, no cleanup job. Releasing the
            // session now means no warehouse or session footprint at steady state.
            LOGGER.info("ingestion_only is on: streaming into {} only, no MERGE and no cleanup job",
                    ingestTableName);
            jdbc.close();
        } else {
            mergeExecutor = new MergeExecutor(jdbc.connection(), tableName, ingestTableName,
                    columnsFinalTable, ingestOnlyColumns);
            startCleanUpJob(config);
        }
    }

    /**
     * The configured ingest-only columns, minus the ones the ingest table does not actually have.
     *
     * <p>The MERGE names them in a {@code SELECT * EXCLUDE (...)}, and Snowflake refuses the whole
     * statement when one of them does not exist - "column 'IH_BLOCKID' does not exist" - so a
     * table without, say, {@code IH_BLOCKID} could never be merged into. Excluding a column that
     * is not there means nothing anyway: the list only says which ingest columns are missing from
     * the final table.
     */
    private List<String> excludedColumnsPresentIn(List<String> ingestColumns, AbstractConfig config) {
        var configured = config.getList(SnowflakeSinkConnector.CFG_EXCLUDE_INGEST_ADDITIONAL_FIELDS)
                .stream().map(c -> c.toUpperCase(Locale.ROOT)).toList();

        var present = configured.stream()
                .filter(c -> ingestColumns.stream().anyMatch(column -> column.equalsIgnoreCase(c)))
                .toList();

        var absent = configured.stream().filter(c -> !present.contains(c)).toList();
        if (!absent.isEmpty()) {
            LOGGER.info("Ignoring {} from '{}': not a column of {}", absent,
                    SnowflakeSinkConnector.CFG_EXCLUDE_INGEST_ADDITIONAL_FIELDS, ingestTableName);
        }
        return present;
    }

    /** The profile decides where the operation, the primary key and the row come from. */
    private RowMapper createRowMapper(String profile, AbstractConfig config, List<String> ingestColumns) {
        var pkFields = config.getList(SnowflakeSinkConnector.CFG_PK_FIELDS);
        var timestampFields = config.getList(SnowflakeSinkConnector.CFG_TIMESTAMP_FIELDS_CONVERT);
        var dateFields = config.getList(SnowflakeSinkConnector.CFG_DATE_FIELDS_CONVERT);
        var timeFields = config.getList(SnowflakeSinkConnector.CFG_TIME_FIELDS_CONVERT);

        var opHeader = config.getString(SnowflakeSinkConnector.CFG_OP_HEADER);
        LOGGER.info("Record format: {}", profile);

        if (SnowflakeSinkConnector.PROFILE_FLAT_JSON.equals(profile)) {
            return new FlatJsonRowMapper(ingestColumns, pkFields, timestampFields, dateFields,
                    timeFields, opHeader);
        }
        if (SnowflakeSinkConnector.PROFILE_CDC_SCHEMA.equals(profile)) {
            return new CdcSchemaRowMapper(ingestColumns, pkFields, timestampFields, dateFields,
                    timeFields);
        }
        return new AutoRowMapper(ingestColumns, pkFields, timestampFields, dateFields, timeFields,
                opHeader);
    }

    /**
     * JDBC takes the account, database and schema from the URL, like v3 does. The streaming SDK
     * cannot - it wants them as arguments - so read them back out of that same URL rather than
     * asking the user to repeat what the connection string already says.
     */
    private void resolveSnowflakeLocation(AbstractConfig config) {
        var url = config.getString(SnowflakeSinkConnector.CFG_URL);

        account = SnowflakeJdbc.accountFrom(url)
                .orElseThrow(() -> new ConnectException(
                        "Could not determine the account from " + SnowflakeSinkConnector.CFG_URL
                                + ". Expected something like "
                                + "jdbc:snowflake://<account>.snowflakecomputing.com"));

        streamingUrl = config.getString(SnowflakeSinkConnector.CFG_STREAMING_URL);
        if (streamingUrl == null || streamingUrl.isBlank()) {
            streamingUrl = SnowflakeJdbc.hostFrom(url)
                    .map(host -> String.format("https://%s:443", host))
                    .orElseThrow(() -> new ConnectException(
                            "Could not determine the streaming endpoint from "
                                    + SnowflakeSinkConnector.CFG_URL + "; set '"
                                    + SnowflakeSinkConnector.CFG_STREAMING_URL + "' explicitly."));
        }

        databaseName = SnowflakeJdbc.urlParam(url, "db", "database")
                .orElseThrow(() -> new ConnectException(
                        "Could not determine the database: add 'db=<DATABASE>' to the "
                                + SnowflakeSinkConnector.CFG_URL + " connection string."));

        schemaName = config.getString(SnowflakeSinkConnector.CFG_SCHEMA_NAME);
        if (schemaName == null || schemaName.isBlank()) {
            schemaName = SnowflakeJdbc.urlParam(url, "schema")
                    .orElseThrow(() -> new ConnectException(
                            "Could not determine the schema: set the '"
                                    + SnowflakeSinkConnector.CFG_SCHEMA_NAME
                                    + "' config or add 'schema=<SCHEMA>' to the "
                                    + SnowflakeSinkConnector.CFG_URL + " connection string."));
        }

        LOGGER.info("Streaming into {}.{}.{} on account {} via {}",
                databaseName, schemaName, ingestTableName, account, streamingUrl);
    }

    /** Overridable so tests can supply a mocked connection. */
    protected SnowflakeJdbc createJdbc(AbstractConfig config) {
        return new SnowflakeJdbc(
                config.getString(SnowflakeSinkConnector.CFG_URL),
                config.getString(SnowflakeSinkConnector.CFG_USER),
                config.getPassword(SnowflakeSinkConnector.CFG_PASSWORD).value(),
                schemaName);
    }

    /** Overridable so tests can supply a mocked streaming client. */
    protected StreamingChannelManager createChannelManager(AbstractConfig config, Map<String, String> map) {
        var client = StreamingChannelManager.buildClient(
                account,
                streamingUrl,
                config.getString(SnowflakeSinkConnector.CFG_USER),
                config.getPassword(SnowflakeSinkConnector.CFG_PASSWORD).value(),
                config.getString(SnowflakeSinkConnector.CFG_ROLE),
                databaseName,
                schemaName,
                ingestTableName,
                config.getString(SnowflakeSinkConnector.CFG_PIPE),
                config.getInt(SnowflakeSinkConnector.CFG_MAX_CLIENT_LAG_SECONDS));

        var prefix = config.getString(SnowflakeSinkConnector.CFG_CHANNEL_NAME_PREFIX);
        if (prefix == null || prefix.isBlank()) {
            prefix = map.getOrDefault("name", ingestTableName);
        }
        return new StreamingChannelManager(client, prefix,
                config.getInt(SnowflakeSinkConnector.CFG_APPEND_MAX_RETRIES));
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        var committed = channels.open(partitions);
        committedAtOpen.putAll(committed);

        // getRowsErrorCount() counts the channel's whole lifetime, not this assignment's. Seed the
        // baseline from what Snowflake already reports, otherwise every restart would replay old
        // rejections as if they were new - and with fail_on_row_error that is a crash loop.
        channels.statuses().forEach((tp, status) -> {
            if (partitions.contains(tp)) {
                rowErrorCounts.put(tp, status.getRowsErrorCount());
                if (status.getRowsErrorCount() > 0) {
                    LOGGER.warn("Channel for {} already reports {} rejected row(s) from before this "
                            + "assignment; only newer rejections will be reported",
                            tp, status.getRowsErrorCount());
                }
            }
        });

        // Only ingestion_only may jump forward to Snowflake's token. In merge mode Kafka's
        // committed offset is the merge watermark and is at or behind Snowflake's, so skipping
        // ahead would strand rows that reached the ingest table but were never merged.
        if (ingestionOnly && !committed.isEmpty()) {
            var rewind = new LinkedHashMap<TopicPartition, Long>();
            committed.forEach((tp, offset) -> rewind.put(tp, offset + 1));
            LOGGER.info("Rewinding consumer to Snowflake committed offsets: {}", rewind);
            context.offset(rewind);
        }

        // the first record delivered on each partition sets the watermark, and Connect resumes from
        // the last merged offset, so that record is exactly the first unmerged one
        partitions.forEach(lastMerged::remove);
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        partitions.forEach(tp -> {
            committedAtOpen.remove(tp);
            rowErrorCounts.remove(tp);
            appendedMax.remove(tp);
            lastMerged.remove(tp);
        });
        if (appendedMax.isEmpty()) {
            blockHasUpserts = false;
            blockHasDeletes = false;
        }
        channels.close(partitions);
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        if (collection.isEmpty()) {
            return;
        }
        LOGGER.debug("PUT - {} records", collection.size());

        for (SinkRecord record : collection) {
            var tp = new TopicPartition(record.topic(), record.kafkaPartition());

            if (!ingestionOnly) {
                // the first record on this partition is the first one not yet merged
                lastMerged.putIfAbsent(tp, record.kafkaOffset() - 1);
            }

            // resolve the primary key before the skip below: after a restart every redelivered
            // record can already be in the ingest table, and the MERGE still needs the key.
            // ingestion_only never merges, so a keyless record is fine there.
            if (!ingestionOnly && pks.isEmpty()) {
                pks = rowMapper.extractPk(record);
                if (!pks.isEmpty()) {
                    LOGGER.info("Primary key columns resolved to: {}", pks);
                }
            }

            var alreadyCommitted = committedAtOpen.get(tp);
            if (alreadyCommitted != null && record.kafkaOffset() <= alreadyCommitted) {
                // Snowflake already has this record - either the rewind in open() had not taken
                // effect yet, or in merge mode these rows are in the ingest table awaiting a merge.
                // Either way it must not be appended twice, but it still belongs to the range.
                if (!ingestionOnly) {
                    appendedMax.merge(tp, record.kafkaOffset(), Math::max);
                    markOperation(record);
                }
                continue;
            }

            var row = rowMapper.toRow(record, currentBatchId);
            try {
                channels.appendRow(tp, row, record.kafkaOffset());
            } catch (SFException e) {
                if (StreamingChannelManager.isChannelConflict(e)) {
                    throw recoverFromConflict(tp, e);
                }
                throw e;
            }

            if (!ingestionOnly) {
                appendedMax.merge(tp, record.kafkaOffset(), Math::max);
                markOperation(record);
            }
        }
    }

    private void markOperation(SinkRecord record) {
        if (RowMapper.Operation.d.name().equalsIgnoreCase(rowMapper.operationOf(record))) {
            blockHasDeletes = true;
        } else {
            blockHasUpserts = true;
        }
    }


    /**
     * A 409 means another client took the channel over. Reopen it, rewind to whatever Snowflake
     * has committed and let Connect redeliver.
     */
    private RetriableException recoverFromConflict(TopicPartition tp, SFException cause) {
        LOGGER.warn("Channel conflict on {}, reopening and rewinding", tp, cause);
        var committed = channels.reopen(tp);
        if (committed != null) {
            committedAtOpen.put(tp, committed);
            context.offset(Map.of(tp, committed + 1));
        } else {
            committedAtOpen.remove(tp);
        }
        appendedMax.remove(tp);
        return new RetriableException("Channel conflict on " + tp + ", reopened and rewound", cause);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
            Map<TopicPartition, OffsetAndMetadata> currentOffsets) {

        var statuses = channels.statuses();
        reportRowErrors(statuses);
        var snowflakeCommitted = channels.committedOffsets(statuses);

        currentBatchId = UUID.randomUUID().toString();

        if (!ingestionOnly) {
            runMergeCycle(snowflakeCommitted);
        }

        // ingestion_only reports what Snowflake holds; merge mode reports the merge watermark, so
        // Kafka never moves past rows that have not reached the final table yet
        var source = ingestionOnly ? snowflakeCommitted : lastMerged;

        var result = new HashMap<TopicPartition, OffsetAndMetadata>();
        source.forEach((tp, committed) -> {
            var current = currentOffsets.get(tp);
            if (current == null) {
                return;
            }
            // never commit past what Connect actually handed us
            var offset = Math.min(committed + 1, current.offset());
            result.put(tp, new OffsetAndMetadata(offset));
        });
        return result;
    }

    /**
     * Waits for the current block to be durable, then applies it to the final table. Bookkeeping
     * is only rotated after both statements succeed, so a failure retries the same block on the
     * next cycle rather than losing it.
     */
    private void runMergeCycle(Map<TopicPartition, Long> snowflakeCommitted) {
        // merge only what Snowflake already holds durably - reading the committed offset token is
        // the confirmation, so there is nothing to wait for
        var ranges = new HashMap<Integer, MergeExecutor.OffsetRange>();
        snowflakeCommitted.forEach((tp, committed) -> {
            var from = lastMerged.get(tp);
            if (from != null && committed > from) {
                ranges.put(tp.partition(), new MergeExecutor.OffsetRange(tp.partition(), from, committed));
            }
        });

        if (ranges.isEmpty()) {
            return;
        }
        if (Duration.between(lastMergeAt, Instant.now()).compareTo(mergeInterval) < 0) {
            // hold the rows in the ingest table and leave the Kafka offsets where they are, so a
            // crash before the next cycle simply replays into the same range
            LOGGER.debug("Holding {} partition(s) until the merge interval of {} elapses",
                    ranges.size(), mergeInterval);
            return;
        }

        if (pks.isEmpty()) {
            // only reachable when the records carry no key at all - the flat JSON profile takes
            // the primary key from the key JSON, and a delete has nothing else to name a row with
            throw new ConnectException("No primary key columns: the records carry no key, so set '"
                    + SnowflakeSinkConnector.CFG_PK_FIELDS
                    + "' to the column(s) that identify a row, or run with ingestion_only=true.");
        }

        if (blockHasUpserts) {
            mergeExecutor.merge(ranges, pks);
        }
        if (blockHasDeletes) {
            mergeExecutor.deleteRows(ranges, pks);
        }

        // only advance once both statements succeeded; a failure leaves the watermark alone and the
        // same range is retried on the next cycle
        ranges.values().forEach(r -> lastMerged.entrySet().stream()
                .filter(e -> e.getKey().partition() == r.partition())
                .forEach(e -> e.setValue(r.toInclusive())));
        lastMergeAt = Instant.now();
        blockHasUpserts = false;
        blockHasDeletes = false;
        appendedMax.clear();
    }

    /**
     * The pipe only supports {@code ON_ERROR = CONTINUE}, so rows Snowflake refuses are dropped
     * into the error table instead of failing the append. Without this they are invisible.
     */
    private void reportRowErrors(Map<TopicPartition, ChannelStatus> statuses) {
        var offenders = new ArrayList<String>();
        for (var entry : statuses.entrySet()) {
            var status = entry.getValue();
            var errors = status.getRowsErrorCount();
            var previous = rowErrorCounts.getOrDefault(entry.getKey(), 0L);
            if (errors > previous) {
                rowErrorCounts.put(entry.getKey(), errors);
                LOGGER.error("Snowflake rejected {} row(s) on {} (total {}), last error: {}",
                        errors - previous, entry.getKey(), errors, status.getLastErrorMessage());
                offenders.add(entry.getKey() + " (" + errors + " rows, " + status.getLastErrorMessage() + ")");
            }
        }
        if (failOnRowError && !offenders.isEmpty()) {
            throw new ConnectException("Snowflake rejected rows and fail_on_row_error is set: "
                    + String.join("; ", offenders));
        }
    }

    protected void startCleanUpJob(AbstractConfig config) {
        if (config.getBoolean(SnowflakeSinkConnector.CFG_JOB_CLEANUP_DISABLE)) {
            LOGGER.warn("Cleanup job is disabled, skipping job creation.");
            return;
        }

        var duration = Duration.parse(config.getString(SnowflakeSinkConnector.CFG_JOB_CLEANUP_DURATION));
        LOGGER.info("Cleanup job will run every {} seconds", duration.toSeconds());

        var jobData = new HashMap<String, Object>();
        jobData.put(CleanupJob.SNOWFLAKE_CONNECTION, jdbc.connection());
        jobData.put(CleanupJob.INGEST_TABLE_NAME, ingestTableName);
        jobData.put(CleanupJob.RETENTION_HOURS,
                config.getInt(SnowflakeSinkConnector.CFG_JOB_CLEANUP_RETENTION_HOURS));

        var props = new Properties();
        props.setProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME, String.format("cleanup_%s", UUID.randomUUID()));
        props.setProperty("org.quartz.threadPool.threadCount", "1");

        try {
            scheduler = new StdSchedulerFactory(props).getScheduler();
            var job = JobBuilder.newJob(CleanupJob.class).withIdentity("cleanupjob")
                    .setJobData(new JobDataMap(jobData))
                    .build();
            var trigger = TriggerBuilder.newTrigger().withIdentity("trigger_cleanupjob")
                    .withSchedule(SimpleScheduleBuilder.repeatSecondlyForever((int) duration.getSeconds()))
                    .build();
            scheduler.scheduleJob(job, trigger);
            scheduler.start();
        } catch (SchedulerException e) {
            LOGGER.error("Error starting cleanup job", e);
            throw new ConnectException("Error starting cleanup job", e);
        }
    }

    @Override
    public void stop() {
        if (scheduler != null) {
            try {
                scheduler.shutdown();
            } catch (SchedulerException e) {
                LOGGER.error("Can not shutdown quartz scheduler", e);
            }
        }
        if (channels != null) {
            channels.close();
        }
        if (jdbc != null) {
            jdbc.close();
        }
    }
}
