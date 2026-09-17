package br.com.datastreambrasil.v4;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every way the JNI load can fail surfaces as the same "Failed to load both main and test
 * libraries", so the connector has to dig the distinguishing detail out of the cause chain.
 * The messages below were reproduced by running the shaded jar under Docker.
 */
class NativeLoadDiagnosisTest {

    private Throwable sdkFailure(String rootMessage) {
        var root = new UnsatisfiedLinkError(rootMessage);
        var wrapped = new RuntimeException("Failed to load both main and test libraries", root);
        return new NoClassDefFoundError("Could not initialize class "
                + "com.snowflake.ingest.streaming.FFIClient") {
            @Override
            public synchronized Throwable getCause() {
                return wrapped;
            }
        };
    }

    @Test
    void namesMuslAsTheCause() {
        var message = StreamingChannelManager.nativeLoadDiagnosis(sdkFailure(
                "/tmp/cyclone_shared123/cyclone_shared.so: Error loading shared library "
                        + "libgcc_s.so.1: No such file or directory"));

        assertTrue(message.contains("Alpine/musl"), message);
        assertTrue(message.contains("glibc 2.26"), message);
    }

    @Test
    void namesNoexecTmpAsTheCause() {
        var message = StreamingChannelManager.nativeLoadDiagnosis(sdkFailure(
                "/tmp/cyclone_shared123/cyclone_shared.so: failed to map segment from shared object"));

        assertTrue(message.contains("noexec"), message);
        assertTrue(message.contains("java.io.tmpdir"), message);
    }

    @Test
    void namesAReadOnlyRootAsTheCause() {
        var message = StreamingChannelManager.nativeLoadDiagnosis(
                sdkFailure("/tmp/cyclone_shared123: Read-only file system"));

        assertTrue(message.contains("not writable"), message);
        assertTrue(message.contains("emptyDir"), message);
    }

    @Test
    void fallsBackToProbingTheEnvironmentWhenTheCauseIsUnfamiliar() {
        var message = StreamingChannelManager.nativeLoadDiagnosis(sdkFailure("something new"));

        // the JVM caches a failed static initializer, so a restarted task never sees the real
        // loader error again - say so instead of leaving the operator with nothing
        assertTrue(message.contains("first time the SDK is touched"), message);
    }

    private static final long PLENTY = 512L * 1024 * 1024;

    @Test
    void namesAnUndersizedTmpDir() {
        // Strimzi mounts /tmp as a memory-backed emptyDir defaulting to 5Mi, and the extracted
        // library is ~29 MiB. Everything else about the environment looks fine, which is exactly
        // what made this one hard to spot.
        var message = StreamingChannelManager.diagnoseEnvironment(
                "/tmp", false, true, 5L * 1024 * 1024, false);

        assertTrue(message.contains("tmpDirSizeLimit"), message);
        assertTrue(message.contains("5 MiB free"), message);
    }

    @Test
    void aRoomyTmpDirIsNotBlamed() {
        var message = StreamingChannelManager.diagnoseEnvironment("/tmp", false, true, PLENTY, false);

        assertFalse(message.contains("tmpDirSizeLimit"), message);
        assertTrue(message.contains("FFIBootstrap"), message);
    }

    @Test
    void ordersTheChecksSoTheMostFundamentalCauseWins() {
        // a musl image with a tiny, read-only tmpdir is still first and foremost a musl image
        assertTrue(StreamingChannelManager.diagnoseEnvironment("/tmp", true, false, 1024, true)
                .contains("Alpine/musl"));
        // and an unwritable tmpdir matters before its size does
        assertTrue(StreamingChannelManager.diagnoseEnvironment("/tmp", false, false, 1024, true)
                .contains("not writable"));
    }

    @Test
    void unknownFreeSpaceIsNotTreatedAsFull() {
        var message = StreamingChannelManager.diagnoseEnvironment("/tmp", false, true, -1, false);

        assertFalse(message.contains("tmpDirSizeLimit"), message);
    }

    @Test
    void picksTheLongestMatchingMountWhenDecidingAboutNoexec() {
        var mounts = """
                overlay / overlay rw,relatime 0 0
                tmpfs /tmp tmpfs rw,noexec,nosuid,size=512m 0 0
                /dev/sdb /tmp/native ext4 rw,relatime 0 0
                """;

        // /tmp itself is noexec
        assertTrue(StreamingChannelManager.isNoexec(mounts, "/tmp"));
        // but a volume mounted underneath it is not, and the longer mount point must win
        assertFalse(StreamingChannelManager.isNoexec(mounts, "/tmp/native"));
        // the root mount is not noexec
        assertFalse(StreamingChannelManager.isNoexec(mounts, "/var/lib/whatever"));
    }

    @Test
    void noexecCheckIsSafeWhenProcMountsIsUnavailable() {
        assertFalse(StreamingChannelManager.isNoexec("", "/tmp"));
        assertFalse(StreamingChannelManager.isNoexec(null, "/tmp"));
        assertFalse(StreamingChannelManager.isNoexec("garbage", null));
    }

    @Test
    void doesNotMistakeAPrefixForAMountPoint() {
        var mounts = "tmpfs /tmpfoo tmpfs rw,noexec 0 0\noverlay / overlay rw 0 0\n";

        // /tmp is not under /tmpfoo despite the shared prefix
        assertFalse(StreamingChannelManager.isNoexec(mounts, "/tmp"));
    }

    @Test
    void alwaysCarriesTheRootCauseAndTheEnvironment() {
        var message = StreamingChannelManager.nativeLoadDiagnosis(
                sdkFailure("/tmp/x.so: failed to map segment from shared object"));

        // the detail that the SDK's own exception throws away
        assertTrue(message.contains("failed to map segment from shared object"), message);
        assertTrue(message.contains("java.io.tmpdir="), message);
        assertTrue(message.contains("arch="), message);
    }
}
