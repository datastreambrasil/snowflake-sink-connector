package br.com.datastreambrasil.v3;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class SnowflakeSinkConnectorTest {

    @Test
    void taskConfigsJobCleanupEnabled() {
        var connector = new SnowflakeSinkConnector();
        connector.start(Map.of(
            SnowflakeSinkConnector.CFG_JOB_CLEANUP_DISABLE, "false"));
        var taskConfigs = connector.taskConfigs(2);
        assertEquals(2, taskConfigs.size());
        assertEquals("false", taskConfigs.get(0).get(SnowflakeSinkConnector.CFG_JOB_CLEANUP_DISABLE));
        assertEquals("true", taskConfigs.get(1).get(SnowflakeSinkConnector.CFG_JOB_CLEANUP_DISABLE));
    }

    @Test
    void taskConfigsJobCleanupDisabled() {
        var connector = new SnowflakeSinkConnector();
        connector.start(Map.of(
            SnowflakeSinkConnector.CFG_JOB_CLEANUP_DISABLE, "true"));
        var taskConfigs = connector.taskConfigs(2);
        assertEquals(2, taskConfigs.size());
        assertEquals("true", taskConfigs.get(0).get(SnowflakeSinkConnector.CFG_JOB_CLEANUP_DISABLE));
        assertEquals("true", taskConfigs.get(1).get(SnowflakeSinkConnector.CFG_JOB_CLEANUP_DISABLE));
    }

    @Test
    void taskConfigsJobCleanupUseDefaultValues() {
        var connector = new SnowflakeSinkConnector();
        connector.start(Map.of());
        var taskConfigs = connector.taskConfigs(3);
        assertEquals(3, taskConfigs.size());
        assertFalse(taskConfigs.get(0).containsKey(SnowflakeSinkConnector.CFG_JOB_CLEANUP_DISABLE));
        assertEquals("true", taskConfigs.get(1).get(SnowflakeSinkConnector.CFG_JOB_CLEANUP_DISABLE));
        assertEquals("true", taskConfigs.get(2).get(SnowflakeSinkConnector.CFG_JOB_CLEANUP_DISABLE));
    }

    @Test
    void version_returnsV3() {
        assertEquals("v3", new SnowflakeSinkConnector().version());
    }

    @Test
    void taskClass_returnsSnowflakeSinkTaskClass() {
        assertSame(SnowflakeSinkTask.class, new SnowflakeSinkConnector().taskClass());
    }

    @Test
    void config_returnsNonNullConfigDef() {
        assertNotNull(new SnowflakeSinkConnector().config());
        assertSame(SnowflakeSinkConnector.CONFIG_DEF, new SnowflakeSinkConnector().config());
    }
}