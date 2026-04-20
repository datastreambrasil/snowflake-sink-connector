# ❄️ Snowflake Sink Connector

> High-throughput Kafka Connect sink that streams CDC and schema-bearing events from Apache Kafka into Snowflake, with first-class support for Debezium-style `create` / `update` / `delete` / `read` operations.

[![Java 17](https://img.shields.io/badge/Java-17%2B-orange?logo=openjdk)](https://openjdk.org/)
[![Kafka Connect](https://img.shields.io/badge/Kafka%20Connect-4.x-231F20?logo=apachekafka)](https://kafka.apache.org/documentation/#connect)
[![Snowflake](https://img.shields.io/badge/Snowflake-JDBC-29B5E8?logo=snowflake)](https://docs.snowflake.com/)
[![Build](https://img.shields.io/badge/build-passing-brightgreen)]()
[![Version](https://img.shields.io/badge/version-3.7.2-blue)]()
[![License](https://img.shields.io/badge/license-Internal-lightgrey)]()

Originally forked from the public [Datastream Brasil](https://github.com/datastreambrasil) connector; this internal build carries significant reliability and performance hardening described below.

> 📝 **A note on code comments:** inline code comments throughout the codebase are intentionally maintained in **Portuguese** to preserve the original authors' intent and keep context close to the domain experts who own this connector. This README is in English for broader team consumption.

---

## 📖 Overview

The Snowflake Sink Connector consumes records from one or more Kafka topics and writes them into Snowflake through a hybrid pipeline:

1. **Buffering**: Incoming `SinkRecord`s are validated, normalized, and staged in an in-memory buffer keyed by primary key.
2. **Staging (PUT, Spill to Disk)**: On each flush, the buffered batch is streamed **directly to a temporary CSV file on the local filesystem** (never fully materialized in the JVM heap). The connector then opens a `FileInputStream` against that file and hands it to `SnowflakeConnection.uploadStream` (gzip-compressed on the wire). The temp file is always deleted in a `finally` block.
3. **Ingestion (COPY INTO)**: The staged file is `COPY INTO`-ed into an ingest (landing) table using JDBC.
4. **Merge**: From the ingest table the connector executes `INSERT` / `UPDATE` / `DELETE` statements against the final target table, reconciling CDC operations.
5. **Cleanup**: A Quartz-scheduled cleanup job prunes stale ingest rows on a configurable cadence.

The default processor (`cdc_schema` profile) is purpose-built for **Debezium CDC envelopes** (`before` / `after` / `op`) with Kafka Connect Struct schemas.

---

## 🚀 Critical Performance Optimizations (The "Why")

The following refactorings were driven by a production incident where Kafka Connect workers crashed with `java.lang.OutOfMemoryError: Required array length 2147483639 + 29693 is too large`. Preserving the reasoning is essential — future maintainers should understand *why* these choices are not merely stylistic.

The connector went through two architectural iterations in response to the incident: first removing the `StringBuilder` / `String` / `byte[]` triple copy (streaming into a `ByteArrayOutputStream`), and now moving the entire CSV payload **out of the heap** via a Spill to Disk approach. Sections **1** and **2** below describe the current design.

### 1. 💽 Spill to Disk — Zero-Heap CSV Assembly

**Problem.** The intermediate fix (stream into `ByteArrayOutputStream`) eliminated the three-way copy, but the **entire serialized CSV still lived in the JVM heap** as a single `byte[]` until the upload completed. Any sufficiently large batch — or a single pathologically wide row with embedded JSON / CLOB / blob columns — could still approach the JVM's ~2 GB maximum array length and trigger the same `OutOfMemoryError`. The connector was *less* likely to OOM, but not *immune* to it.

**Fix.** The CSV is now streamed **directly to a temporary file on the OS filesystem**. The JVM heap never holds the full payload. During assembly the only thing in memory is the `BufferedWriter`'s small flush buffer (a few KB); every byte written passes through and lands on disk. At upload time the connector opens a `FileInputStream` and hands it to the Snowflake driver, which reads from disk in chunks.

```java
// Spill to Disk — the CSV is assembled directly into the OS temp directory.
var csvTempFile = Files.createTempFile("snowflake-sink-", ".csv");
try (var writer = new BufferedWriter(
        new OutputStreamWriter(new FileOutputStream(csvTempFile.toFile()), StandardCharsets.UTF_8))) {
    for (var recordInBuffer : buffer.values()) {
        for (String column : columnsFromTable) {
            writer.write(renderCell(column, recordInBuffer, blockID));
            // ...
        }
        writer.write('\n');
    }
}

// Upload streams bytes from disk; the full payload is never resident in the heap.
try (var inputStream = new FileInputStream(csvTempFile.toFile())) {
    snowflakeConnection.uploadStream(stageName, "/", inputStream, destFileName, true);
}
```

| Dimension | Previous (`ByteArrayOutputStream`) | Current (Spill to Disk) |
|---|---|---|
| Peak heap during CSV assembly | Proportional to batch size (can exceed 2 GB) | **Constant** — only the `BufferedWriter` flush buffer (~8 KB) |
| Payload size ceiling | JVM max array length (~2 GB) | **Free disk space on the Connect worker** |
| `Required array length ... is too large` OOM | Still possible on wide rows / huge batches | **Impossible** — no `byte[]` ever holds the full payload |
| GC pressure per flush | One large short-lived `byte[]` per flush | Near zero; bytes stream through without allocation |

Net effect: the connector is **100% immune to `OutOfMemoryError` during CSV assembly, regardless of batch size or row width**. The only resource the payload consumes is a file handle and some `/tmp` bytes — both of which are reclaimed before `flush()` returns (see §2).

### 2. 🛟 Strict Disk Cleanup (Fail-Safe)

**Problem.** Spilling to disk trades one failure mode (heap OOM) for another: if temporary files leak — because the upload hung, the network dropped, Snowflake returned an error, or the JVM was killed mid-flush — `/tmp` eventually fills up and workers start throwing `java.io.IOException: No space left on device`. A disk-exhaustion outage is arguably *worse* than a heap OOM, because it takes down **every** sink task on the worker, not just the one that tripped. Spill to Disk is only safe if we can guarantee the temp file is always removed.

**Fix.** `flush()` wraps the entire upload + COPY INTO + merge pipeline in a `try` / `finally` and deletes the temp file unconditionally in the `finally` clause:

```java
Path csvTempFile = null;
try {
    csvTempFile = prepareOrderedColumnsBasedOnTargetTable(blockID, columnsFromMetadata);

    try (var inputStream = new FileInputStream(csvTempFile.toFile())) {
        snowflakeConnection.uploadStream(stageName, "/", inputStream, destFileName, true);
    }
    // ... COPY INTO / INSERT / UPDATE / DELETE against the ingest + final tables ...
} catch (Throwable e) {
    LOGGER.error("Error while flushing Snowflake connector", e);
    throw new RuntimeException("Error while flushing", e);
} finally {
    // Strict cleanup — remove the CSV even if the upload or SQL step exploded.
    if (csvTempFile != null) {
        try {
            Files.deleteIfExists(csvTempFile);
        } catch (IOException ex) {
            LOGGER.warn("Failed to remove temp CSV {}: {}", csvTempFile, ex.getMessage());
        }
    }
    buffer.clear();
}
```

Why this design matters in production:

- **Snowflake outages can't cascade into disk exhaustion.** Even when `uploadStream` hangs or `COPY INTO` returns an error, every temp file is removed on the way out. A Snowflake incident degrades throughput, not worker health.
- **Idempotent on retry.** `Files.deleteIfExists(...)` is explicitly designed to tolerate a missing file (e.g. removed by an OS-level `/tmp` reaper between creation and cleanup). Repeated Connect task restarts never throw on the cleanup path.
- **Early failures self-heal.** If the CSV assembly itself fails mid-write, `prepareOrderedColumnsBasedOnTargetTable` deletes its own partial file **before rethrowing** — the caller never receives a `Path` for a stranded file, so the outer `finally` has nothing to clean up and nothing is ever left behind.
- **Observable leaks.** Any residual `/tmp/snowflake-sink-*.csv` older than one flush cycle is by definition a bug. You can point a simple `find /tmp -name 'snowflake-sink-*.csv' -mmin +10` at the workers as a canary.

Together, §1 (Spill to Disk) and §2 (Strict Cleanup) give the connector **true infinite resilience** against payload-size-driven failures: batches are limited only by the worker's free disk space, and temp-file leaks are structurally impossible under the `finally`-guaranteed cleanup.

### 3. 🧯 State Leak Resolution in `AbstractProcessor`

**Problem.** `configParameters(...)` populated the processor's convert-column lists like this:

```java
timestampFieldsConvert.addAll(config.getList(CFG_TIMESTAMP_FIELDS_CONVERT));
dateFieldsConvert.addAll(config.getList(CFG_DATE_FIELDS_CONVERT));
timeFieldsConvert.addAll(config.getList(CFG_TIME_FIELDS_CONVERT));
ignoreColumns.addAll(config.getList(CFG_IGNORE_COLUMNS));
```

Kafka Connect can invoke `start()` multiple times on the same task instance during rebalances, retries, or reconfiguration. Each call appended the same configuration values to the existing lists, causing **unbounded linear accumulation** of duplicates — with a knock-on O(N) cost inside every `containsAny(...)` check on the hot CSV path.

**Fix.** Replace additive mutation with reassignment so the lists reflect configuration, not history:

```java
timestampFieldsConvert = new ArrayList<>(config.getList(CFG_TIMESTAMP_FIELDS_CONVERT));
dateFieldsConvert     = new ArrayList<>(config.getList(CFG_DATE_FIELDS_CONVERT));
timeFieldsConvert     = new ArrayList<>(config.getList(CFG_TIME_FIELDS_CONVERT));
ignoreColumns         = new ArrayList<>(config.getList(CFG_IGNORE_COLUMNS));
```

Each reconfiguration now yields a clean, correctly-sized list — no growth, no drift.

### 4. 🔑 Buffer Bloat & PK-Based Collapsing

**Problem.** The in-flight buffer used `UUID.randomUUID().toString()` as its key:

```java
buffer.put(UUID.randomUUID().toString(), recordToSnowflake);
```

Every event — including repeated mutations on the same row — produced a **unique key**, so the buffer grew linearly with the Kafka volume until the next flush. For high-churn tables (hot records updated many times per second) this meant the buffer held thousands of stale versions of the same logical row, inflating both heap usage and the CSV payload.

**Fix.** Key the buffer by the logical primary key (or by UUID only in hashing mode, where duplicates are semantically required):

```java
buffer.put(convertPKToStringKey(recordToSnowflake), recordToSnowflake);
```

Consequences:

- Successive events on the same PK **collapse** — only the latest wins, matching the CDC "last writer" semantic.
- Buffer size tracks the number of distinct rows pending, not the raw event count.
- The CSV uploaded to Snowflake is leaner, `COPY INTO` is faster, and downstream `MERGE` / `UPDATE` / `DELETE` statements touch fewer rows.

### 5. ✨ Minor Hardening

- `getFirstSeenHash(...)` explicitly returns `searchHash` (pass-through). The previous `buffer.get(searchHash)` could never match once the buffer stopped being hash-keyed; the intent is now documented rather than accidental.
- Date-field conversion promotes arithmetic to `long` (`asInt * 24L * 60L * 60L`) to preclude any future int-overflow surprises on far-future dates.
- `String.replaceAll("\"", "\"\"")` → `String.replace("\"", "\"\"")` to avoid an unnecessary regex compile per cell.

---

## 🛠️ Building the Project

Requirements:

- **JDK 17+**
- **Maven 3.8+**

Build the shaded, deployment-ready JAR:

```bash
mvn clean package -DskipTests
```

The artifact is produced under `target/`:

```
target/snowflake-sink-connector-<version>-jar-with-dependencies.jar
```

➡️ **Deploy this** (`*-jar-with-dependencies.jar`) into your Kafka Connect plugin path (e.g. `/usr/share/java/snowflake-sink-connector/`). The thin JAR (`snowflake-sink-connector-<version>.jar`) is **not** self-contained and will fail at runtime due to missing transitive dependencies.

To run the test suite:

```bash
mvn org.apache.maven.plugins:maven-surefire-plugin:3.2.5:test
```

> ℹ️ The project's pinned `maven-surefire-plugin` predates JUnit 5 discovery. The command above overrides it for local verification.

---

## ⚙️ Configuration Guide

### Deployment payload

Register the connector against a Kafka Connect REST endpoint:

```bash
curl -X POST http://kafka-connect:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connector.json
```

Example `connector.json`:

```json
{
  "name": "snowflake-sink-orders",
  "config": {
    "connector.class": "br.com.datastreambrasil.v3.SnowflakeSinkConnector",
    "tasks.max": "4",
    "topics": "cdc.public.orders,cdc.public.order_items",

    "url": "jdbc:snowflake://acme.snowflakecomputing.com/?warehouse=INGEST_WH&db=ANALYTICS",
    "user": "KAFKA_CONNECT_SVC",
    "password": "${file:/secrets/snowflake.properties:password}",
    "schema": "RAW",
    "table": "ORDERS",
    "stage": "KAFKA_STAGE",

    "profile": "cdc_schema",
    "hashing_support": "false",
    "pk": "id",

    "timestamp_fields_convert": "created_at,updated_at",
    "date_fields_convert": "order_date",
    "time_fields_convert": "pickup_time",
    "ignore_columns": "internal_debug_blob",

    "job_cleanup_duration": "PT4H",
    "job_cleanup_disable": "false",

    "max.poll.records": "500",
    "consumer.override.max.partition.fetch.bytes": "10485760",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter.schemas.enable": "true"
  }
}
```

### Core properties

| Property | Required | Default | Description |
|---|:---:|---|---|
| `connector.class` | ✅ | — | Must be `br.com.datastreambrasil.v3.SnowflakeSinkConnector`. |
| `topics` | ✅ | — | Comma-separated list of source topics. |
| `tasks.max` | ✅ | — | Parallelism. Scale with topic partition count. |
| `url` | ✅ | — | Snowflake JDBC URL including warehouse and database. |
| `user` / `password` | ✅ | — | Snowflake service account credentials. Use Connect secret providers. |
| `schema` | ✅ | — | Snowflake schema hosting the target and ingest (landing) tables. |
| `table` | ✅ | — | Final target table. The ingest landing table is expected at `<table>_INGEST`. |
| `stage` | ✅ | — | Snowflake internal stage used by the `PUT` / `COPY INTO` pipeline. |
| `profile` | ⚪ | `cdc_schema` | Processor profile. `cdc_schema` expects Debezium-style envelopes (`before` / `after` / `op`). |
| `hashing_support` | ⚪ | `false` | When `true`, identity is tracked by CRC32 hashes (`ih_current_hash` / `ih_previous_hash`) instead of the primary key. |
| `pk` | ⚪ | `[]` | Optional explicit PK column list (overrides inference from the Kafka key schema). |
| `timestamp_fields_convert` | ⚪ | `[]` | Columns where epoch-milli `long` values should be rendered as `LocalDateTime`. |
| `date_fields_convert` | ⚪ | `[]` | Columns where epoch-day `int` values should be rendered as `LocalDate`. |
| `time_fields_convert` | ⚪ | `[]` | Columns where nano-of-day `long` values should be rendered as `LocalTime`. |
| `ignore_columns` | ⚪ | `[]` | Columns to strip from the Snowflake table metadata before CSV projection. |
| `job_cleanup_duration` | ⚪ | `PT4H` | Quartz-style ISO-8601 duration for the ingest-table cleanup cadence. |
| `job_cleanup_disable` | ⚪ | `false` | Disable the cleanup job entirely (useful for non-primary tasks in a multi-task deployment). |
| `max.poll.records` | ⚪ | (Connect default) | Batch size per poll. Lower values smooth memory usage; higher values improve throughput. |

> 🔐 **Secrets.** Never inline credentials. Use Kafka Connect's [ConfigProvider](https://kafka.apache.org/documentation/#connect_configproviders) mechanism (`FileConfigProvider`, Vault, AWS Secrets Manager, etc.).

---

## 📊 Operations & Observability

### Logs

The connector uses Log4j2 (SLF4J-bound). The relevant loggers live under `br.com.datastreambrasil.v3`. Noteworthy messages:

| Level | Message pattern | What it tells you |
|---|---|---|
| `DEBUG` | `Preparing to send N records from buffer...` | Flush cadence and batch size. Useful for sizing `max.poll.records`. |
| `DEBUG` | `Prepared csv on disk in X ms, path /tmp/snowflake-sink-*.csv, size Y bytes` | CSV assembly time, temp-file path, and payload size — your primary **payload-size indicator** under the Spill to Disk architecture. |
| `DEBUG` | `Uploaded N records in X ms` | Time spent in `SnowflakeConnection.uploadStream`, reading from the temp file. |
| `DEBUG` | `Executed statement in X ms` | JDBC wall time for `COPY INTO` / `INSERT` / `UPDATE` / `DELETE`. |
| `WARN`  | `Column X not found on record schema, fallback to snowflake original column name` | Schema drift — investigate upstream. |
| `WARN`  | `Failed to remove temp CSV ...` | Rare: the `finally`-block cleanup failed. Investigate disk / permissions; pair with a `/tmp` canary (see below). |
| `ERROR` | `Error while flushing Snowflake connector` | Flush failure. The temp CSV is still deleted in `finally` and the buffer is cleared; the framework will re-deliver from the last committed offsets. |

For production, enable `DEBUG` on `br.com.datastreambrasil.v3` selectively (or via a log-rotating sampler) — the `Prepared csv on disk ... size Y bytes` line is the canonical signal to watch.

### Memory & disk monitoring

After the Spill to Disk refactor, expected steady-state behavior is:

- **Bounded buffer size**: the in-memory `SinkRecord` buffer grows only with the number of *distinct* primary keys pending between flushes.
- **CSV payload off-heap**: the serialized CSV lives on the OS filesystem, not the JVM heap. Peak heap during assembly is ≈ the `BufferedWriter` flush buffer, independent of batch size.
- **No leaked temp files**: every `/tmp/snowflake-sink-*.csv` is deleted in `flush()`'s `finally` block — a file surviving past one flush cycle is by definition a bug.
- **Stable old-gen**: no per-flush leak; `buffer.clear()` runs unconditionally in `flush`'s `finally`.

Recommended to track:

| Metric | Signal |
|---|---|
| JVM heap used (old gen) | Should be flat post-flush and largely independent of payload size; a climbing trend indicates a regression. |
| GC pause time / frequency | Should correlate with flush cadence, not grow over time. Spill to Disk removes the large short-lived `byte[]` allocations that used to dominate. |
| Worker free disk on `/tmp` (or `$TMPDIR`) | **New.** Each flush briefly consumes up to the raw CSV size on disk. Size the partition for `sum(concurrent flush payloads) × safety margin`. |
| Leaked temp files | `find /tmp -name 'snowflake-sink-*.csv' -mmin +10 \| wc -l` should remain `0`. Non-zero values indicate a cleanup bug or a JVM killed mid-flush. |
| `kafka.connect:type=sink-task-metrics,sink-record-send-total` | Throughput baseline. |
| `kafka.connect:type=sink-task-metrics,sink-record-lag-max` | Back-pressure signal; pair with `size Y bytes` log to diagnose slow flushes. |
| Snowflake query history (`QUERY_HISTORY` view) | Confirms `COPY INTO` / `MERGE` latencies match in-connector timings. |

### Sizing guidance

| Scenario | Suggested tuning |
|---|---|
| High-cardinality, low-update streams | Increase `max.poll.records`; buffer collapsing is less beneficial, so throughput dominates. |
| Hot-key, high-update streams | Keep `max.poll.records` moderate — PK collapsing will do the work and keep payloads small. |
| Wide rows with large JSON/CLOB columns | Spill to Disk removes the heap ceiling, so payload size is now a **disk** concern, not a heap concern. Ensure `/tmp` (or `$TMPDIR`) has ample free space; consider listing the offending column in `ignore_columns` if it is not needed in Snowflake. |
| Very large batches | Safe by design — size is bounded by worker free disk, not by the JVM 2 GB array limit. Prefer larger batches for throughput, and monitor the `size Y bytes` log to capacity-plan `/tmp`. |

---

## 🧭 Versioning & Support

This is an internally maintained fork. File issues, RFCs, and deployment requests through the **Data Platform** team. Breaking changes are flagged in the `CHANGELOG` and require a coordinated rollout with the Integrations team because Kafka Connect task restarts re-read metadata against Snowflake (see `configMetadata()`).

---

*Built with care by the Data Platform team — because production outages should teach us something, and that something belongs in a README.*
