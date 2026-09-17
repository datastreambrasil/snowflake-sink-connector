package br.com.datastreambrasil.v4;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.ChannelStatusBatch;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StreamingChannelManagerTest {

    private static final TopicPartition TP = new TopicPartition("cdc.test.dbo.events", 2);

    private SnowflakeStreamingIngestClient client;
    private SnowflakeStreamingIngestChannel channel;
    private StreamingChannelManager manager;

    @BeforeEach
    void setUp() {
        client = mock(SnowflakeStreamingIngestClient.class);
        channel = mock(SnowflakeStreamingIngestChannel.class);
        when(client.openChannel(anyString()))
                .thenReturn(new OpenChannelResult(channel, mock(ChannelStatus.class)));
        manager = new StreamingChannelManager(client, "sink-snowflake", 3);
    }

    @Test
    void channelNamesAreStableAndFreeOfTopicDots() {
        // dots are not valid in a channel name and the name must be reproducible across restarts
        assertEquals("sink-snowflake_cdc_test_dbo_events_2", manager.channelName(TP));
        assertEquals(manager.channelName(TP), manager.channelName(new TopicPartition(TP.topic(), 2)));
    }

    @Test
    void openReusesTheExistingOffsetTokenInsteadOfResettingIt() {
        when(channel.getLatestCommittedOffsetToken()).thenReturn("117");

        var committed = manager.open(List.of(TP));

        // the two-argument openChannel would overwrite the token and break exactly-once
        verify(client).openChannel("sink-snowflake_cdc_test_dbo_events_2");
        verify(client, never()).openChannel(anyString(), anyString());
        assertEquals(117L, committed.get(TP));
    }

    @Test
    void openReportsNoOffsetForAChannelThatNeverCommitted() {
        when(channel.getLatestCommittedOffsetToken()).thenReturn(null);

        assertTrue(manager.open(List.of(TP)).isEmpty());
    }

    @Test
    void appendRowUsesTheKafkaOffsetAsTheOffsetToken() {
        manager.open(List.of(TP));
        var row = Map.<String, Object>of("ID", "1");

        manager.appendRow(TP, row, 4321L);

        verify(channel).appendRow(row, "4321");
    }

    @Test
    void appendRowRetriesTransientFailures() {
        manager.open(List.of(TP));
        var throttled = mock(SFException.class);
        when(throttled.getHttpStatusCode()).thenReturn(429);
        doThrow(throttled).doThrow(throttled).doNothing()
                .when(channel).appendRow(anyMap(), anyString());

        manager.appendRow(TP, Map.of("ID", "1"), 1L);

        verify(channel, times(3)).appendRow(anyMap(), eq("1"));
    }

    @Test
    void appendRowDoesNotRetryPermanentFailures() {
        manager.open(List.of(TP));
        var badRequest = mock(SFException.class);
        when(badRequest.getHttpStatusCode()).thenReturn(400);
        doThrow(badRequest).when(channel).appendRow(anyMap(), anyString());

        assertThrows(SFException.class, () -> manager.appendRow(TP, Map.of("ID", "1"), 1L));
        verify(channel, times(1)).appendRow(anyMap(), anyString());
    }

    @Test
    void appendingToAnUnopenedPartitionFails() {
        assertThrows(IllegalStateException.class, () -> manager.appendRow(TP, Map.of(), 1L));
    }

    @Test
    void statusesAreFetchedInOneCallAndMappedBackToPartitions() {
        manager.open(List.of(TP));
        var status = mock(ChannelStatus.class);
        when(status.getLatestCommittedOffsetToken()).thenReturn("500");
        when(client.getChannelStatus(any()))
                .thenReturn(new ChannelStatusBatch(Map.of("sink-snowflake_cdc_test_dbo_events_2", status)));

        var statuses = manager.statuses();

        assertEquals(status, statuses.get(TP));
        assertEquals(500L, manager.committedOffsets(statuses).get(TP));
        verify(client, times(1)).getChannelStatus(any());
    }

    @Test
    void nonNumericOffsetTokensAreIgnoredRatherThanCrashing() {
        manager.open(List.of(TP));
        var status = mock(ChannelStatus.class);
        when(status.getLatestCommittedOffsetToken()).thenReturn("not-a-number");
        when(client.getChannelStatus(any()))
                .thenReturn(new ChannelStatusBatch(Map.of("sink-snowflake_cdc_test_dbo_events_2", status)));

        assertNull(manager.committedOffsets(manager.statuses()).get(TP));
    }

    @Test
    void closingAPartitionReleasesItsChannel() {
        manager.open(List.of(TP));
        doNothing().when(channel).close();

        manager.close(List.of(TP));

        verify(channel).close();
        assertThrows(IllegalStateException.class, () -> manager.channel(TP));
    }

    @Test
    void conflictIsRecognisedAsAChannelTakeover() {
        var conflict = mock(SFException.class);
        when(conflict.getHttpStatusCode()).thenReturn(409);

        assertTrue(StreamingChannelManager.isChannelConflict(conflict));
        assertTrue(!StreamingChannelManager.isRetryable(conflict));
    }
}
