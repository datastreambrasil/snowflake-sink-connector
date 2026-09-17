package br.com.datastreambrasil.v4;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnowflakeSinkConnectorTest {

    private SnowflakeSinkConnector startedConnector() {
        var connector = new SnowflakeSinkConnector();
        connector.start(Map.of(
                "schema", "LZ_IH",
                "table", "EVENTS",
                "password", "pat-secret"));
        return connector;
    }

    @Test
    void onlyTheFirstTaskRunsTheCleanupJob() {
        var configs = startedConnector().taskConfigs(3);

        assertEquals(3, configs.size());
        assertNull(configs.get(0).get(SnowflakeSinkConnector.CFG_JOB_CLEANUP_DISABLE));
        assertEquals("true", configs.get(1).get(SnowflakeSinkConnector.CFG_JOB_CLEANUP_DISABLE));
        assertEquals("true", configs.get(2).get(SnowflakeSinkConnector.CFG_JOB_CLEANUP_DISABLE));
    }

    @Test
    void versionIsV4() {
        assertEquals("v4", new SnowflakeSinkConnector().version());
    }

    @Test
    void stageAndBufferSettingsAreGoneFromV4() {
        var keys = SnowflakeSinkConnector.CONFIG_DEF.names();

        assertFalse(keys.contains("stage"));
        assertFalse(keys.contains("tmp_data_folder"));
        assertFalse(keys.contains("buffer_initial_capacity"));
        assertFalse(keys.contains("copy_only"));
    }

    @Test
    void authenticationKeepsTheV3Shape() {
        var keys = SnowflakeSinkConnector.CONFIG_DEF.names();

        // v3 already passes the PAT as the password, so a v3 definition carries over unchanged
        assertTrue(keys.contains("user"));
        assertTrue(keys.contains("password"));
        assertFalse(keys.contains("personal_access_token"));
        assertFalse(keys.contains("jdbc_authenticator"));

        // like v3, everything the connection string already says is read back from it
        // instead of being asked for a second time
        assertTrue(keys.contains("url"));
        assertFalse(keys.contains("account"));
        assertFalse(keys.contains("database"));
    }

    @Test
    void neitherRecordFormatHasToBeConfigured() {
        var keys = SnowflakeSinkConnector.CONFIG_DEF.names();

        assertTrue(keys.contains("profile"));
        assertTrue(keys.contains("op_header"));
        assertTrue(keys.contains("pk_fields"));

        // the whole point: a connector definition says nothing about the format, and both a
        // Debezium envelope and a flat JSON row work
        assertEquals("auto",
                SnowflakeSinkConnector.CONFIG_DEF.configKeys().get("profile").defaultValue);
        assertEquals("op",
                SnowflakeSinkConnector.CONFIG_DEF.configKeys().get("op_header").defaultValue);
    }

    @Test
    void streamingSettingsAreDeclared() {
        var keys = SnowflakeSinkConnector.CONFIG_DEF.names();

        assertTrue(keys.contains("ingestion_only"));
        assertTrue(keys.contains("pipe"));
        assertNotNull(SnowflakeSinkConnector.CONFIG_DEF.configKeys().get("ingestion_only").defaultValue);
        assertEquals(false, SnowflakeSinkConnector.CONFIG_DEF.configKeys().get("ingestion_only").defaultValue);
    }
}
