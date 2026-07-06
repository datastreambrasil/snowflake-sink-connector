package br.com.datastreambrasil.v3;

import org.apache.kafka.common.config.AbstractConfig;
import org.junit.jupiter.api.Test;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class CdcDbzSchemaProcessorStopTest {

    private Scheduler schedulerFieldOf(CdcDbzSchemaProcessor p) throws Exception {
        Field f = CdcDbzSchemaProcessor.class.getDeclaredField("scheduler");
        f.setAccessible(true);
        return (Scheduler) f.get(p);
    }

    private void setScheduler(CdcDbzSchemaProcessor p, Scheduler scheduler) throws Exception {
        Field f = CdcDbzSchemaProcessor.class.getDeclaredField("scheduler");
        f.setAccessible(true);
        f.set(p, scheduler);
    }

    private CdcDbzSchemaProcessor processorWithMockConnection() {
        var p = new CdcDbzSchemaProcessor();
        p.connection = mock(Connection.class);
        p.knownIngestTables = ConcurrentHashMap.newKeySet();
        return p;
    }

    private AbstractConfig configWithCleanupEnabled() {
        return new AbstractConfig(SnowflakeSinkConnector.CONFIG_DEF, Map.of(
                "schema", "MY_SCHEMA",
                "table", "MY_TABLE",
                "stage", "MY_STAGE"
        ));
    }

    // ── stop ─────────────────────────────────────────────────────────────────

    @Test
    void stop_withNullScheduler_doesNotThrow() {
        var p = new CdcDbzSchemaProcessor();
        assertDoesNotThrow(p::stop, "stop() with no scheduler started should be a no-op");
    }

    @Test
    void stop_withRunningScheduler_shutsDownGracefully() throws Exception {
        var p = processorWithMockConnection();
        p.startCleanUpJob(configWithCleanupEnabled());
        assertNotNull(schedulerFieldOf(p), "Scheduler should be set after startCleanUpJob");

        assertDoesNotThrow(p::stop, "stop() should shut down the scheduler without throwing");
    }

    @Test
    void stop_whenSchedulerThrows_exceptionSwallowed() throws Exception {
        var p = new CdcDbzSchemaProcessor();
        var mockScheduler = mock(Scheduler.class);
        doThrow(new SchedulerException("forced error")).when(mockScheduler).shutdown();
        setScheduler(p, mockScheduler);

        assertDoesNotThrow(p::stop,
                "SchedulerException in stop() must be caught and not re-thrown");
        verify(mockScheduler).shutdown();
    }

    // ── startCleanUpJob ───────────────────────────────────────────────────────

    @Test
    void startCleanUpJob_whenDisabled_schedulerRemainsNull() throws Exception {
        var p = processorWithMockConnection();
        var config = new AbstractConfig(SnowflakeSinkConnector.CONFIG_DEF, Map.of(
                "schema", "MY_SCHEMA",
                "table", "MY_TABLE",
                "stage", "MY_STAGE",
                "job_cleanup_disable", "true"
        ));

        p.startCleanUpJob(config);

        assertNull(schedulerFieldOf(p),
                "Scheduler must not be created when cleanup job is disabled");
    }
}
