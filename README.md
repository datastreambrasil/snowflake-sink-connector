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
2. **Staging (PUT)**: On each flush, the buffered batch is serialized as CSV and uploaded to a Snowflake internal stage via the `SnowflakeConnection.uploadStream` API (gzip-compressed on the wire).
3. **Ingestion (COPY INTO)**: The staged file is `COPY INTO`-ed into an ingest (landing) table using JDBC.
4. **Merge**: From the ingest table the connector executes `INSERT` / `UPDATE` / `DELETE` statements against the final target table, reconciling CDC operations.
5. **Cleanup**: A Quartz-scheduled cleanup job prunes stale ingest rows on a configurable cadence.

The default processor (`cdc_schema` profile) is purpose-built for **Debezium CDC envelopes** (`before` / `after` / `op`) with Kafka Connect Struct schemas.

---

## 🚀 Critical Performance Optimizations (The "Why")

The following refactorings were driven by a production incident where Kafka Connect workers crashed with `java.lang.OutOfMemoryError: Required array length 2147483639 + 29693 is too large`. Preserving the reasoning is essential — future maintainers should understand *why* these choices are not merely stylistic.

### 1. 🧵 Stream-Based CSV Assembly

**Problem.** The previous `prepareOrderedColumnsBasedOnTargetTable` accumulated the entire batch CSV inside a `StringBuilder`, then materialized it with `stringBuilder.toString().getBytes(UTF_8)` before handing the bytes to a `ByteArrayOutputStream`.

This has three compounding costs:

| Stage | Memory footprint |
|---|---|
| `StringBuilder` internal `char[]` | **2 bytes per character**, doubles on overflow (up to ~2× live data during resize) |
| `toString()` | allocates a fresh `String` → another full copy of the `char[]` |
| `getBytes(UTF_8)` | allocates a `byte[]` → yet another full copy |

For a single wide CDC row containing a large JSON / CLOB / serialized blob column, the transient peak could trivially exceed the JVM's ~2 GB maximum array length, producing the observed OOM even with `max.poll.records=1`.

**Fix.** We now stream each cell directly into the output buffer:

```java
try (var writer = new BufferedWriter(new OutputStreamWriter(csvInMemory, StandardCharsets.UTF_8))) {
    for (var recordInBuffer : buffer.values()) {
        for (String column : columnsFromTable) {
            writer.write(renderCell(column, recordInBuffer, blockID));
            // ...
        }
        writer.write('\n');
    }
}
```

Benefits:

- **No `char[]` → `String` → `byte[]` triple copy.** Bytes are written as they are produced.
- **UTF-8 bytes instead of UTF-16 chars.** For the ASCII-dominant CSV payload this roughly halves peak heap.
- **No `StringBuilder` doubling.** `ByteArrayOutputStream` grows geometrically too, but from a smaller base and without an intermediate `toString()` pivot.

Net effect: the single-record OOM ceiling is raised dramatically, and steady-state heap for large batches is materially lower.

### 2. 🧯 State Leak Resolution in `AbstractProcessor`

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

### 3. 🔑 Buffer Bloat & PK-Based Collapsing

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

### 4. ✨ Minor Hardening

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
| `DEBUG` | `Prepared csv in memory in X ms, size Y bytes` | CSV assembly time and payload size — your primary **memory-pressure indicator**. |
| `DEBUG` | `Uploaded N records in X ms` | Time spent in `SnowflakeConnection.uploadStream`. |
| `DEBUG` | `Executed statement in X ms` | JDBC wall time for `COPY INTO` / `INSERT` / `UPDATE` / `DELETE`. |
| `WARN`  | `Column X not found on record schema, fallback to snowflake original column name` | Schema drift — investigate upstream. |
| `ERROR` | `Error while flushing Snowflake connector` | Flush failure. The buffer is cleared in `finally`; the framework will re-deliver from the last committed offsets. |

For production, enable `DEBUG` on `br.com.datastreambrasil.v3` selectively (or via a log-rotating sampler) — the `Prepared csv in memory ... size Y bytes` line is the canonical signal to watch.

### Memory monitoring

After the refactor, expected steady-state behavior is:

- **Bounded buffer size**: grows only with the number of *distinct* primary keys pending between flushes.
- **Bounded CSV payload**: no transient `StringBuilder`/`String`/`byte[]` triplication.
- **Stable old-gen**: no per-flush leak; `buffer.clear()` runs unconditionally in `flush`'s `finally`.

Recommended to track:

| Metric | Signal |
|---|---|
| JVM heap used (old gen) | Should be flat post-flush; a climbing trend indicates a regression. |
| GC pause time / frequency | Should correlate with flush cadence, not grow over time. |
| `kafka.connect:type=sink-task-metrics,sink-record-send-total` | Throughput baseline. |
| `kafka.connect:type=sink-task-metrics,sink-record-lag-max` | Back-pressure signal; pair with `size Y bytes` log to diagnose slow flushes. |
| Snowflake query history (`QUERY_HISTORY` view) | Confirms `COPY INTO` / `MERGE` latencies match in-connector timings. |

### Sizing guidance

| Scenario | Suggested tuning |
|---|---|
| High-cardinality, low-update streams | Increase `max.poll.records`; buffer collapsing is less beneficial, so throughput dominates. |
| Hot-key, high-update streams | Keep `max.poll.records` moderate — PK collapsing will do the work and keep payloads small. |
| Wide rows with large JSON/CLOB columns | Lower `max.poll.records`, and consider listing the offending column in `ignore_columns` if it is not needed in Snowflake. |

---

## 🧭 Versioning & Support

This is an internally maintained fork. File issues, RFCs, and deployment requests through the **Data Platform** team. Breaking changes are flagged in the `CHANGELOG` and require a coordinated rollout with the Integrations team because Kafka Connect task restarts re-read metadata against Snowflake (see `configMetadata()`).

---

*Built with care by the Data Platform team — because production outages should teach us something, and that something belongs in a README.*
