package br.com.datastreambrasil.v3;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class SnowflakeSinkTaskTest {

    private static AbstractProcessor injectMockProcessor(SnowflakeSinkTask task) throws Exception {
        var mockProcessor = mock(AbstractProcessor.class);
        Field field = SnowflakeSinkTask.class.getDeclaredField("processor");
        field.setAccessible(true);
        field.set(task, mockProcessor);
        return mockProcessor;
    }

    @Test
    void version_returnsConnectorVersion() {
        assertEquals(SnowflakeSinkConnector.VERSION, new SnowflakeSinkTask().version());
    }

    @Test
    void start_withUnknownProfile_throwsRuntimeException() {
        var task = new SnowflakeSinkTask();
        var ex = assertThrows(RuntimeException.class, () -> task.start(Map.of(
                "schema", "test_schema",
                "table", "test_table",
                "stage", "test_stage",
                "profile", "unknown_profile"
        )));
        assertTrue(ex.getMessage().contains("unknown_profile"),
                "Exception message should name the unknown profile");
    }

    @Test
    void put_delegatesToProcessor() throws Exception {
        var task = new SnowflakeSinkTask();
        var processor = injectMockProcessor(task);
        Collection<SinkRecord> records = List.of(mock(SinkRecord.class));

        task.put(records);

        verify(processor).put(records);
    }

    @Test
    void flush_delegatesToProcessor() throws Exception {
        var task = new SnowflakeSinkTask();
        var processor = injectMockProcessor(task);
        var offsets = Map.<TopicPartition, OffsetAndMetadata>of();

        task.flush(offsets);

        verify(processor).flush(offsets);
    }

    @Test
    void stop_delegatesToProcessor() throws Exception {
        var task = new SnowflakeSinkTask();
        var processor = injectMockProcessor(task);

        task.stop();

        verify(processor).stop();
    }
}
