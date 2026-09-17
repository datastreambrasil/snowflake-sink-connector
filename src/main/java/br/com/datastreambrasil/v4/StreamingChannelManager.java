package br.com.datastreambrasil.v4;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Owns the Snowpipe Streaming client and one channel per Kafka topic-partition.
 *
 * <p>Channel names are derived from the topic-partition and are stable across restarts and
 * rebalances, which is what lets Snowflake remember the offset token for that partition and give
 * us exactly-once delivery.
 */
public class StreamingChannelManager implements AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(StreamingChannelManager.class);

    /** Snowflake auto-creates this pipe for a table the first time a channel is opened on it. */
    protected static final String DEFAULT_PIPE_SUFFIX = "-STREAMING";

    protected static final int HTTP_REQUEST_TIMEOUT = 408;
    protected static final int HTTP_TOO_MANY_REQUESTS = 429;
    protected static final int HTTP_CONFLICT = 409;
    protected static final int HTTP_INTERNAL_SERVER_ERROR = 500;
    protected static final int HTTP_SERVICE_UNAVAILABLE = 503;

    private final SnowflakeStreamingIngestClient client;
    private final String channelNamePrefix;
    private final int maxRetries;

    private final Map<TopicPartition, SnowflakeStreamingIngestChannel> channels = new HashMap<>();
    private final Map<TopicPartition, String> channelNames = new HashMap<>();

    public StreamingChannelManager(SnowflakeStreamingIngestClient client, String channelNamePrefix,
                                   int maxRetries) {
        this.client = client;
        this.channelNamePrefix = sanitize(channelNamePrefix);
        this.maxRetries = maxRetries;
    }

    /**
     * Builds the SDK client. {@code pipe} may be null, in which case the default pipe is used.
     *
     * <p>{@code password} is the same value v2/v3 hand to JDBC: the programmatic access token.
     * JDBC accepts it as a plain password, but the SDK has no password concept and must be told
     * the authorization type explicitly.
     */
    public static SnowflakeStreamingIngestClient buildClient(String account, String streamingUrl,
                                                            String user, String password,
                                                            String role, String database,
                                                            String schema, String ingestTable,
                                                            String pipe, Integer maxClientLagSeconds) {
        var props = new Properties();
        props.put("account", account);
        props.put("url", streamingUrl);
        props.put("authorization_type", "PAT");
        props.put("personal_access_token", password);
        if (user != null && !user.isBlank()) {
            props.put("user", user);
        }
        if (role != null && !role.isBlank()) {
            props.put("role", role);
        }

        var pipeName = pipe != null && !pipe.isBlank() ? pipe : ingestTable + DEFAULT_PIPE_SUFFIX;
        var clientName = String.format("kc_%s_%s", ingestTable, UUID.randomUUID());

        LOGGER.info("Creating Snowpipe Streaming client {} on {}.{} pipe {}",
                clientName, database, schema, pipeName);

        var builder = SnowflakeStreamingIngestClientFactory
                .builder(clientName, database, schema, pipeName)
                .setProperties(props);

        if (maxClientLagSeconds != null) {
            builder.setParameterOverrides(Map.of("max_client_lag_seconds", maxClientLagSeconds));
        }

        try {
            return builder.build();
        } catch (NoClassDefFoundError | UnsatisfiedLinkError | ExceptionInInitializerError e) {
            throw new ConnectException(nativeLoadDiagnosis(e), e);
        }
    }

    /**
     * The SDK loads a Rust core over JNI by extracting it from the jar into a temp directory and
     * calling System.load on it. Every way that can fail surfaces as the same
     * "Failed to load both main and test libraries", so the message alone tells you nothing - the
     * detail that distinguishes them sits further down the cause chain, and is lost entirely once
     * a later access reports "Could not initialize class FFIClient" instead.
     *
     * <p>Pull the root cause up into the message and say what each variant means.
     */
    protected static String nativeLoadDiagnosis(Throwable error) {
        var root = error;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        var detail = String.valueOf(root.getMessage());

        // Only the first attempt in a JVM carries the real loader error. Once the static
        // initializer has failed, the JVM marks the class erroneous and every later attempt gets
        // "Could not initialize class FFIClient" with the cause gone - which is what a restarted
        // task sees. Fall back to inspecting the environment ourselves.
        var likelyCause = diagnoseFromMessage(detail);
        if (likelyCause == null) {
            likelyCause = diagnoseFromEnvironment()
                    + " (the loader error itself is only logged the first time the SDK is touched "
                    + "in a JVM; restart the worker to see it again)";
        }

        var tmpDir = System.getProperty("java.io.tmpdir");
        return String.format(
                "Could not load the Snowpipe Streaming native library. %s "
                        + "[os=%s, arch=%s, java.io.tmpdir=%s, writable=%s, rootCause=%s: %s]",
                likelyCause, System.getProperty("os.name"), System.getProperty("os.arch"),
                tmpDir, isWritable(tmpDir), root.getClass().getName(), detail);
    }

    /** Reads the cause off the loader error, when we still have it. Null when unrecognised. */
    protected static String diagnoseFromMessage(String detail) {
        if (detail.contains("libgcc_s.so") || detail.contains("musl") || detail.contains("Error relocating")) {
            return "The image looks like Alpine/musl. The SDK needs glibc 2.26 or newer; "
                    + "rebuild the Connect image on a glibc base (UBI or Debian).";
        }
        if (detail.contains("failed to map segment")) {
            return "java.io.tmpdir is mounted noexec, so the extracted library cannot be mapped. "
                    + "Point -Djava.io.tmpdir at a writable, exec-allowed volume.";
        }
        if (detail.contains("Read-only file system") || detail.contains("Permission denied")) {
            return "java.io.tmpdir is not writable - typically a read-only root filesystem. "
                    + "Mount an emptyDir and point -Djava.io.tmpdir at it.";
        }
        return null;
    }

    /** The extracted Rust core is about 29 MiB; leave room for the extraction itself. */
    protected static final long REQUIRED_TMP_BYTES = 48L * 1024 * 1024;

    /** Works out the likely cause from the running environment, for when the cause chain is gone. */
    protected static String diagnoseFromEnvironment() {
        var tmpDir = System.getProperty("java.io.tmpdir");
        return diagnoseEnvironment(tmpDir, isMusl(), isWritable(tmpDir), usableSpace(tmpDir),
                isNoexec(readMounts(), tmpDir));
    }

    /** The decision itself, separated from the probing so every branch can be tested. */
    protected static String diagnoseEnvironment(String tmpDir, boolean musl, boolean writable,
                                                long usableBytes, boolean noexec) {
        if (musl) {
            return "This looks like an Alpine/musl image. The SDK needs glibc 2.26 or newer; "
                    + "rebuild the Connect image on a glibc base (UBI or Debian).";
        }
        if (!writable) {
            return "java.io.tmpdir (" + tmpDir + ") is not writable - typically a read-only root "
                    + "filesystem. Mount an emptyDir and point -Djava.io.tmpdir at it.";
        }
        if (usableBytes >= 0 && usableBytes < REQUIRED_TMP_BYTES) {
            return String.format(
                    "java.io.tmpdir (%s) has only %d MiB free but the native library needs about "
                            + "%d MiB to be extracted. On Strimzi /tmp is a memory-backed emptyDir "
                            + "defaulting to 5Mi: raise it with "
                            + "spec.template.pod.tmpDirSizeLimit (128Mi is plenty), or point "
                            + "-Djava.io.tmpdir at a larger volume.",
                    tmpDir, usableBytes / (1024 * 1024), REQUIRED_TMP_BYTES / (1024 * 1024));
        }
        if (noexec) {
            return "java.io.tmpdir (" + tmpDir + ") is on a noexec mount, so the extracted library "
                    + "cannot be mapped. Point -Djava.io.tmpdir at a writable, exec-allowed volume.";
        }
        return "The environment looks usable from here, so check the WARN line from "
                + "com.snowflake.ingest.streaming.FFIBootstrap in the worker log.";
    }

    private static long usableSpace(String path) {
        try {
            return path == null ? -1 : new java.io.File(path).getUsableSpace();
        } catch (SecurityException e) {
            return -1;
        }
    }

    private static boolean isMusl() {
        try (var libs = java.nio.file.Files.list(java.nio.file.Path.of("/lib"))) {
            return libs.anyMatch(p -> p.getFileName().toString().startsWith("ld-musl-"));
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean isWritable(String path) {
        try {
            return path != null && new java.io.File(path).canWrite();
        } catch (SecurityException e) {
            return false;
        }
    }

    private static String readMounts() {
        try {
            return java.nio.file.Files.readString(java.nio.file.Path.of("/proc/mounts"));
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * True when {@code path} sits on a mount carrying the noexec option. Picks the longest matching
     * mount point, since /tmp and / can both be listed.
     */
    protected static boolean isNoexec(String mounts, String path) {
        if (mounts == null || mounts.isBlank() || path == null) {
            return false;
        }

        var best = "";
        var bestNoexec = false;
        for (String line : mounts.split("\n")) {
            var parts = line.trim().split("\\s+");
            if (parts.length < 4) {
                continue;
            }
            var mountPoint = parts[1];
            var under = path.equals(mountPoint)
                    || path.startsWith(mountPoint.endsWith("/") ? mountPoint : mountPoint + "/");
            if (under && mountPoint.length() >= best.length()) {
                best = mountPoint;
                bestNoexec = java.util.Arrays.asList(parts[3].split(",")).contains("noexec");
            }
        }
        return bestNoexec;
    }

    /**
     * Opens a channel per partition, reusing whatever offset token Snowflake already has for it.
     *
     * @return the committed offset per partition, absent when the channel has never committed
     */
    public Map<TopicPartition, Long> open(Collection<TopicPartition> partitions) {
        var committed = new LinkedHashMap<TopicPartition, Long>();
        for (TopicPartition tp : partitions) {
            var name = channelName(tp);
            // the single-argument overload keeps the channel's existing offset token; the
            // two-argument one would reset it and break exactly-once on rebalance
            var channel = client.openChannel(name).getChannel();
            channels.put(tp, channel);

            var token = channel.getLatestCommittedOffsetToken();
            LOGGER.info("Opened channel {} for {} with committed offset token {}", name, tp, token);
            parseOffset(token).ifPresent(offset -> committed.put(tp, offset));
        }
        return committed;
    }

    /** Closes the channels of the given partitions and forgets them. */
    public void close(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            var channel = channels.remove(tp);
            channelNames.remove(tp);
            if (channel == null) {
                continue;
            }
            try {
                channel.close();
            } catch (Exception e) {
                LOGGER.warn("Error while closing channel for {}", tp, e);
            }
        }
    }

    /** Reopens a partition's channel, used to recover from a channel conflict (HTTP 409). */
    public Long reopen(TopicPartition tp) {
        close(List.of(tp));
        return open(List.of(tp)).get(tp);
    }

    /**
     * Appends one row, retrying transient failures with exponential backoff.
     *
     * <p>The offset token is the Kafka offset, so a replay of already-committed records is
     * discarded by Snowflake instead of being written twice.
     */
    public void appendRow(TopicPartition tp, Map<String, Object> row, long kafkaOffset) {
        var channel = channel(tp);
        var offsetToken = Long.toString(kafkaOffset);

        SFException lastError = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                channel.appendRow(row, offsetToken);
                return;
            } catch (SFException e) {
                if (!isRetryable(e)) {
                    throw e;
                }
                lastError = e;
                var backoffMillis = Math.min(30_000L, (long) (100L * Math.pow(2, attempt)));
                LOGGER.warn("Retryable error appending to {} (attempt {}/{}, http {}), backing off {} ms",
                        tp, attempt + 1, maxRetries + 1, e.getHttpStatusCode(), backoffMillis, e);
                try {
                    Thread.sleep(backoffMillis);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while retrying appendRow", ie);
                }
            }
        }
        throw new RuntimeException("Giving up appending to " + tp + " after " + (maxRetries + 1)
                + " attempts", lastError);
    }

    public static boolean isRetryable(SFException e) {
        var status = e.getHttpStatusCode();
        return status == HTTP_REQUEST_TIMEOUT
                || status == HTTP_TOO_MANY_REQUESTS
                || status == HTTP_INTERNAL_SERVER_ERROR
                || status == HTTP_SERVICE_UNAVAILABLE;
    }

    public static boolean isChannelConflict(SFException e) {
        return e.getHttpStatusCode() == HTTP_CONFLICT;
    }

    /** Waits until Snowflake has committed everything up to {@code targetOffset} on that partition. */
    public CompletableFuture<Void> waitForCommit(TopicPartition tp, long targetOffset, Duration timeout) {
        return channel(tp).waitForCommit(
                token -> parseOffset(token).filter(offset -> offset >= targetOffset).isPresent(),
                timeout);
    }

    /**
     * Status of every open channel, fetched in a single call. Carries both the committed offset
     * token and the rejected-row counters, so one round trip covers offset commit and error
     * monitoring.
     */
    public Map<TopicPartition, ChannelStatus> statuses() {
        var result = new LinkedHashMap<TopicPartition, ChannelStatus>();
        if (channels.isEmpty()) {
            return result;
        }

        var names = channels.keySet().stream().map(this::channelName).toList();
        var batch = client.getChannelStatus(names).getChannelStatusBatch();
        for (TopicPartition tp : channels.keySet()) {
            var status = batch.get(channelName(tp));
            if (status != null) {
                result.put(tp, status);
            }
        }
        return result;
    }

    /** Committed offset per partition, absent when the channel has never committed. */
    public Map<TopicPartition, Long> committedOffsets(Map<TopicPartition, ChannelStatus> statuses) {
        var offsets = new LinkedHashMap<TopicPartition, Long>();
        statuses.forEach((tp, status) ->
                parseOffset(status.getLatestCommittedOffsetToken()).ifPresent(offset -> offsets.put(tp, offset)));
        return offsets;
    }

    public ChannelStatus status(TopicPartition tp) {
        return channel(tp).getChannelStatus();
    }

    public SnowflakeStreamingIngestChannel channel(TopicPartition tp) {
        var channel = channels.get(tp);
        if (channel == null) {
            throw new IllegalStateException("No open channel for " + tp
                    + ". Partitions must be opened before use.");
        }
        return channel;
    }

    public String channelName(TopicPartition tp) {
        return channelNames.computeIfAbsent(tp, key -> String.format("%s_%s_%d",
                channelNamePrefix, sanitize(key.topic()), key.partition()));
    }

    private static Optional<Long> parseOffset(String token) {
        if (token == null || token.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Long.parseLong(token.trim()));
        } catch (NumberFormatException e) {
            LOGGER.warn("Ignoring non-numeric offset token '{}'", token);
            return Optional.empty();
        }
    }

    /** Channel names only accept a restricted character set; topic names carry dots. */
    protected static String sanitize(String value) {
        return value == null ? "" : value.replaceAll("[^A-Za-z0-9_-]", "_");
    }

    @Override
    public void close() {
        close(List.copyOf(channels.keySet()));
        try {
            client.close();
        } catch (Exception e) {
            LOGGER.warn("Error while closing Snowpipe Streaming client", e);
        }
    }
}
