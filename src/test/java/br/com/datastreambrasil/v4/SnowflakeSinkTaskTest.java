package br.com.datastreambrasil.v4;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.SFException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SnowflakeSinkTaskTest {

    private static final TopicPartition TP = new TopicPartition("cdc.test.dbo.events", 0);
    private static final List<String> INGEST_COLUMNS = List.of(
            "ID", "NAME", "IH_TOPIC", "IH_PARTITION", "IH_OFFSET", "IH_OP", "IH_DATETIME", "IH_BLOCKID");

    private static Schema payloadSchema;
    private static Schema valueSchema;
    private static Schema keySchema;

    private SnowflakeJdbc jdbc;
    private StreamingChannelManager channels;
    private SinkTaskContext context;
    private Connection connection;
    private Statement statement;

    @BeforeAll
    static void beforeAll() {
        payloadSchema = SchemaBuilder.struct()
                .field("Id", Schema.STRING_SCHEMA)
                .field("Name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        valueSchema = SchemaBuilder.struct()
                .field("before", payloadSchema)
                .field("after", payloadSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();
        keySchema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).build();
    }

    @BeforeEach
    void setUp() throws SQLException {
        jdbc = mock(SnowflakeJdbc.class);
        channels = mock(StreamingChannelManager.class);
        context = mock(SinkTaskContext.class);
        connection = mock(Connection.class);
        statement = mock(Statement.class);

        when(jdbc.columnsOf(anyString(), anyBoolean(), anyList())).thenReturn(INGEST_COLUMNS);
        when(jdbc.connection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeLargeUpdate(anyString())).thenReturn(1L);
        when(channels.statuses()).thenReturn(Map.of());
        when(channels.committedOffsets(anyMap())).thenReturn(Map.of());
    }

    private SnowflakeSinkTask task(Map<String, String> overrides) {
        var props = new HashMap<String, String>();
        props.put("name", "sink-snowflake");
        props.put("schema", "LZ_IH");
        props.put("table", "EVENTS");
        props.put("user", "svc_user");
        props.put("password", "pat-secret");
        props.put("url", "jdbc:snowflake://xy12345.snowflakecomputing.com?db=DATB200D&schema=LZ_IH");
        props.put("job_cleanup_disable", "true");
        props.putAll(overrides);

        var task = new TestTask(jdbc, channels);
        task.initialize(context);
        task.start(props);
        return task;
    }

    private SinkRecord record(String op, String id, long offset) {
        var payload = new Struct(payloadSchema).put("Id", id).put("Name", "Name " + id);
        var value = new Struct(valueSchema).put("op", op);
        if ("d".equals(op)) {
            value.put("before", payload);
        } else {
            value.put("after", payload);
        }
        return new SinkRecord(TP.topic(), TP.partition(), keySchema,
                new Struct(keySchema).put("id", id), valueSchema, value, offset);
    }

    private ChannelStatus status(String committedToken, long rowsErrorCount) {
        var status = mock(ChannelStatus.class);
        when(status.getLatestCommittedOffsetToken()).thenReturn(committedToken);
        when(status.getRowsErrorCount()).thenReturn(rowsErrorCount);
        return status;
    }

    @Test
    void ingestionOnlyStreamsRowsAndNeverTouchesTheFinalTable() throws SQLException {
        var task = task(Map.of("ingestion_only", "true"));

        task.put(List.of(record("c", "1", 0L), record("u", "2", 1L), record("d", "3", 2L)));
        task.preCommit(Map.of(TP, new OffsetAndMetadata(3L)));

        verify(channels, times(3)).appendRow(eq(TP), anyMap(), anyLong());
        verify(connection, never()).createStatement();
        // the JDBC session is released after column discovery: no warehouse at steady state
        verify(jdbc).close();
    }

    @Test
    void openRewindsTheConsumerToWhatSnowflakeAlreadyCommitted() {
        when(channels.open(any())).thenReturn(Map.of(TP, 117L));
        var task = task(Map.of("ingestion_only", "true"));

        task.open(List.of(TP));

        verify(context).offset(Map.of(TP, 118L));
    }

    @Test
    void putDropsRecordsSnowflakeAlreadyHas() {
        when(channels.open(any())).thenReturn(Map.of(TP, 5L));
        var task = task(Map.of("ingestion_only", "true"));
        task.open(List.of(TP));

        task.put(List.of(record("c", "1", 4L), record("c", "2", 5L), record("c", "3", 6L)));

        verify(channels, times(1)).appendRow(eq(TP), anyMap(), anyLong());
        verify(channels).appendRow(eq(TP), anyMap(), eq(6L));
    }

    @Test
    void preCommitReportsSnowflakeCommittedOffsetsNotKafkaProgress() {
        var status = status("41", 0L);
        when(channels.statuses()).thenReturn(Map.of(TP, status));
        when(channels.committedOffsets(anyMap())).thenReturn(Map.of(TP, 41L));
        var task = task(Map.of("ingestion_only", "true"));

        var offsets = task.preCommit(Map.of(TP, new OffsetAndMetadata(100L)));

        assertEquals(42L, offsets.get(TP).offset());
    }

    @Test
    void preCommitNeverCommitsPastWhatConnectDelivered() {
        var status = status("999", 0L);
        when(channels.statuses()).thenReturn(Map.of(TP, status));
        when(channels.committedOffsets(anyMap())).thenReturn(Map.of(TP, 999L));
        var task = task(Map.of("ingestion_only", "true"));

        var offsets = task.preCommit(Map.of(TP, new OffsetAndMetadata(10L)));

        assertEquals(10L, offsets.get(TP).offset());
    }

    @Test
    void rejectionsFromBeforeThisAssignmentAreNotReplayedOnRestart() {
        // getRowsErrorCount() is cumulative for the channel's whole life. Without a baseline
        // taken at open(), a restart would re-report old rejections as new - and with
        // fail_on_row_error that means the task fails again the moment it comes back.
        var status = status("10", 7L);
        when(channels.statuses()).thenReturn(Map.of(TP, status));
        var task = task(Map.of("ingestion_only", "true", "fail_on_row_error", "true"));

        task.open(List.of(TP));

        task.preCommit(Map.of(TP, new OffsetAndMetadata(11L)));
    }

    @Test
    void rejectionsAfterTheBaselineStillFailTheTask() {
        var atOpen = status("10", 7L);
        when(channels.statuses()).thenReturn(Map.of(TP, atOpen));
        var task = task(Map.of("ingestion_only", "true", "fail_on_row_error", "true"));
        task.open(List.of(TP));

        var later = status("20", 9L);
        when(channels.statuses()).thenReturn(Map.of(TP, later));

        var e = assertThrows(ConnectException.class,
                () -> task.preCommit(Map.of(TP, new OffsetAndMetadata(21L))));
        assertTrue(e.getMessage().contains("rejected rows"), e.getMessage());
    }

    @Test
    void rejectedRowsFailTheTaskWhenAsked() {
        var status = status("1", 7L);
        when(channels.statuses()).thenReturn(Map.of(TP, status));
        var task = task(Map.of("ingestion_only", "true", "fail_on_row_error", "true"));

        var e = assertThrows(ConnectException.class,
                () -> task.preCommit(Map.of(TP, new OffsetAndMetadata(2L))));
        assertTrue(e.getMessage().contains("rejected rows"), e.getMessage());
    }

    /** Tells the mocked channels that Snowflake has durably committed up to {@code offset}. */
    private void snowflakeHasUpTo(long offset) {
        var status = status(Long.toString(offset), 0L);
        when(channels.statuses()).thenReturn(Map.of(TP, status));
        when(channels.committedOffsets(anyMap())).thenReturn(Map.of(TP, offset));
    }

    @Test
    void mergeModeAppliesWhatSnowflakeAlreadyHolds() throws SQLException {
        var task = task(Map.of("ingestion_only", "false"));
        task.put(List.of(record("c", "1", 0L), record("d", "2", 1L)));
        snowflakeHasUpTo(1L);

        var offsets = task.preCommit(Map.of(TP, new OffsetAndMetadata(2L)));

        // the range is merged, and only then does Kafka advance past it
        verify(statement).executeLargeUpdate(matches("(?s)MERGE INTO EVENTS.*"));
        verify(statement).executeLargeUpdate(matches("(?s)DELETE FROM EVENTS.*"));
        assertEquals(2L, offsets.get(TP).offset());
    }

    @Test
    void mergeModeOnlyRunsTheStatementsTheRangeNeeds() throws SQLException {
        var task = task(Map.of("ingestion_only", "false"));
        task.put(List.of(record("c", "1", 0L)));
        snowflakeHasUpTo(0L);

        task.preCommit(Map.of(TP, new OffsetAndMetadata(1L)));

        verify(statement).executeLargeUpdate(matches("(?s)MERGE INTO EVENTS.*"));
        verify(statement, never()).executeLargeUpdate(matches("(?s)DELETE FROM EVENTS.*"));
    }

    @Test
    void mergeIntervalHoldsTheCycleAndKafkaWaitsWithIt() throws SQLException {
        var task = task(Map.of("ingestion_only", "false", "merge_interval", "PT1H"));

        // first cycle always runs, so a restart with a backlog is not stalled by the interval
        task.put(List.of(record("c", "1", 0L)));
        snowflakeHasUpTo(0L);
        var first = task.preCommit(Map.of(TP, new OffsetAndMetadata(1L)));
        verify(statement, times(1)).executeLargeUpdate(matches("(?s)MERGE INTO EVENTS.*"));
        assertEquals(1L, first.get(TP).offset());

        // second cycle is inside the interval: no statement, and Kafka stays on the watermark so a
        // crash simply replays into the same range
        task.put(List.of(record("c", "2", 1L), record("c", "3", 2L)));
        snowflakeHasUpTo(2L);
        var held = task.preCommit(Map.of(TP, new OffsetAndMetadata(3L)));

        verify(statement, times(1)).executeLargeUpdate(matches("(?s)MERGE INTO EVENTS.*"));
        assertEquals(1L, held.get(TP).offset());
    }

    @Test
    void mergeModeDoesNotSkipRowsThatReachedIngestButWereNotMerged() throws SQLException {
        // simulates a restart: Snowflake's channel is ahead of Kafka's committed offset because
        // the previous run appended rows and died before merging them
        when(channels.open(any())).thenReturn(Map.of(TP, 5L));
        var task = task(Map.of("ingestion_only", "false"));
        task.open(List.of(TP));

        // merge mode must not jump the consumer forward to Snowflake's token
        verify(context, never()).offset(anyMap());

        // Connect redelivers from its own committed offset; those rows are already in the ingest
        // table, so they are not appended again but still belong to the range
        task.put(List.of(record("c", "1", 3L), record("c", "2", 4L), record("c", "3", 5L)));
        verify(channels, never()).appendRow(any(), anyMap(), anyLong());

        snowflakeHasUpTo(5L);
        var offsets = task.preCommit(Map.of(TP, new OffsetAndMetadata(6L)));

        verify(statement).executeLargeUpdate(matches("(?s)MERGE INTO EVENTS.*"));
        assertEquals(6L, offsets.get(TP).offset());
    }

    @Test
    void channelConflictReopensTheChannelAndRewinds() {
        var conflict = mock(SFException.class);
        when(conflict.getHttpStatusCode()).thenReturn(409);
        when(channels.reopen(TP)).thenReturn(88L);
        var task = task(Map.of("ingestion_only", "true"));
        doThrow(conflict).when(channels).appendRow(any(), anyMap(), anyLong());

        assertThrows(RetriableException.class, () -> task.put(List.of(record("c", "1", 100L))));

        verify(channels).reopen(TP);
        verify(context).offset(Map.of(TP, 89L));
    }

    /** A flat JSON record: the row in the value, the operation in a header. */
    private SinkRecord flatRecord(String op, String id, boolean withKey, long offset) {
        var value = new LinkedHashMap<String, Object>();
        value.put("ID", id);
        value.put("NAME", "Name " + id);

        var record = new SinkRecord(TP.topic(), TP.partition(), null,
                withKey ? Map.of("ID", id) : null, null,
                "d".equals(op) ? null : value, offset);
        record.headers().addString("op", op);
        return record;
    }

    @Test
    void anExcludedColumnTheIngestTableDoesNotHaveIsLeftOutOfTheStatement() throws SQLException {
        // Snowflake refuses the whole MERGE when SELECT * EXCLUDE names a column that is not
        // there, so an ingest table without IH_BLOCKID could never be merged into
        when(jdbc.columnsOf(anyString(), anyBoolean(), anyList()))
                .thenReturn(List.of("ID", "NAME", "IH_TOPIC", "IH_PARTITION", "IH_OFFSET", "IH_OP",
                        "IH_DATETIME"));

        var task = task(Map.of("ingestion_only", "false"));
        task.put(List.of(record("c", "1", 0L)));
        snowflakeHasUpTo(0L);
        task.preCommit(Map.of(TP, new OffsetAndMetadata(1L)));

        verify(statement).executeLargeUpdate(matches("(?s).*EXCLUDE \\(IH_TOPIC,IH_PARTITION,IH_OFFSET,IH_OP,IH_DATETIME\\).*"));
    }

    @Test
    void withNothingConfiguredBothFormatsGoThroughTheSameTask() {
        // no 'profile' in the config at all: the default detects each record
        var task = task(Map.of("ingestion_only", "true"));

        task.put(List.of(record("c", "1", 0L), flatRecord("c", "2", true, 1L),
                record("d", "3", 2L), flatRecord("d", "4", true, 3L)));

        verify(channels, times(4)).appendRow(eq(TP), anyMap(), anyLong());
    }

    @Test
    void flatJsonProfileStreamsAndMergesOnTheKeyFromTheRecord() throws SQLException {
        var task = task(Map.of("profile", "flat_json", "ingestion_only", "false"));

        task.put(List.of(flatRecord("c", "1", true, 0L), flatRecord("d", "2", true, 1L)));
        snowflakeHasUpTo(1L);
        var offsets = task.preCommit(Map.of(TP, new OffsetAndMetadata(2L)));

        verify(channels, times(2)).appendRow(eq(TP), anyMap(), anyLong());
        // the key JSON names the primary key, exactly as the Debezium key schema does
        verify(statement).executeLargeUpdate(matches("(?s)MERGE INTO EVENTS.*final.ID = ingest.ID.*"));
        verify(statement).executeLargeUpdate(matches("(?s)DELETE FROM EVENTS.*"));
        assertEquals(2L, offsets.get(TP).offset());
    }

    @Test
    void flatJsonWithoutKeysSaysWhichSettingIsMissing() {
        var task = task(Map.of("profile", "flat_json", "ingestion_only", "false"));

        task.put(List.of(flatRecord("c", "1", false, 0L)));
        snowflakeHasUpTo(0L);

        var e = assertThrows(ConnectException.class,
                () -> task.preCommit(Map.of(TP, new OffsetAndMetadata(1L))));
        assertTrue(e.getMessage().contains("pk_fields"), e.getMessage());
    }

    @Test
    void flatJsonWithoutKeysMergesOnTheConfiguredPkFields() throws SQLException {
        var task = task(Map.of("profile", "flat_json", "ingestion_only", "false",
                "pk_fields", "ID"));

        task.put(List.of(flatRecord("c", "1", false, 0L)));
        snowflakeHasUpTo(0L);
        task.preCommit(Map.of(TP, new OffsetAndMetadata(1L)));

        verify(statement).executeLargeUpdate(matches("(?s)MERGE INTO EVENTS.*final.ID = ingest.ID.*"));
    }

    @Test
    void flatJsonInIngestionOnlyNeedsNoKeyAtAll() {
        var task = task(Map.of("profile", "flat_json", "ingestion_only", "true"));

        task.put(List.of(flatRecord("c", "1", false, 0L), flatRecord("u", "2", false, 1L)));

        verify(channels, times(2)).appendRow(eq(TP), anyMap(), anyLong());
    }

    @Test
    void unknownProfileIsRejected() {
        assertThrows(ConnectException.class, () -> task(Map.of("profile", "cdc_schemaless")));
    }

    @Test
    void aUrlWithoutADatabaseIsRejectedWithAnActionableMessage() {
        var e = assertThrows(ConnectException.class,
                () -> task(Map.of("url", "jdbc:snowflake://xy12345.snowflakecomputing.com")));

        assertTrue(e.getMessage().contains("db=<DATABASE>"), e.getMessage());
    }

    @Test
    void schemaFallsBackToTheUrlWhenNotConfigured() {
        // no 'schema' config: it has to come out of the URL, as the database already does
        var props = new HashMap<String, String>();
        
        props.put("table", "EVENTS");
        props.put("user", "svc_user");
        props.put("password", "pat-secret");
        props.put("url", "jdbc:snowflake://xy12345.snowflakecomputing.com?db=DATB200D&schema=LZ_IH");
        props.put("ingestion_only", "true");

        var task = new TestTask(jdbc, channels);
        task.initialize(context);
        task.start(props);

        verify(jdbc).columnsOf(eq("EVENTS_INGEST"), anyBoolean(), anyList());
    }

    private static final class TestTask extends SnowflakeSinkTask {
        private final SnowflakeJdbc jdbc;
        private final StreamingChannelManager channels;

        private TestTask(SnowflakeJdbc jdbc, StreamingChannelManager channels) {
            this.jdbc = jdbc;
            this.channels = channels;
        }

        @Override
        protected SnowflakeJdbc createJdbc(AbstractConfig config) {
            return jdbc;
        }

        @Override
        protected StreamingChannelManager createChannelManager(AbstractConfig config, Map<String, String> map) {
            return channels;
        }
    }
}
