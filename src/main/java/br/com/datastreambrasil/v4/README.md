Documentação do conector v4. Ele vive neste pacote e não altera o v2 nem o v3, que
continuam no mesmo jar, cada um no seu pacote:
`br.com.datastreambrasil.v4.SnowflakeSinkConnector`.

# v4 — Snowpipe Streaming

v4 drops the PUT-to-stage and COPY steps entirely. Rows are streamed row by row into
`<table>_INGEST` through the [Snowpipe Streaming high-performance
architecture](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-overview),
which is serverless and billed per uncompressed GB ingested instead of by warehouse time and
cloud services.

Two modes:

- **`ingestion_only: true`** — rows land in `_INGEST` and nothing else runs. No MERGE, no cleanup
  job, no warehouse at steady state. Deduplication is yours to do in Snowflake, typically with a
  Dynamic Table that keeps the newest event per key and drops the keys whose last event was a
  delete (see [Deduplicating with a Dynamic Table](#deduplicating-with-a-dynamic-table)).
- **`ingestion_only: false`** — the connector also applies what Snowflake already holds to the
  final table with MERGE/DELETE, like v3 did. `merge_interval` controls how often that happens:
  the default merges on every Connect commit (60s), and raising it to `PT15M` cuts the cycles by
  15x. The statement costs nearly the same for one row as for a million — 0.67s vs 0.75s measured —
  so what drives the bill is how many cycles run, not how much data moves.

### Prerequisites

- **A programmatic access token (PAT)**, supplied through `password` exactly as v2/v3 already do,
  so an existing connector definition carries over unchanged. JDBC takes the token as a plain
  password; the streaming SDK has no password concept, so the connector forwards the same value
  to it as `authorization_type=PAT`. A PAT for a `TYPE = SERVICE` user only works while that user
  is covered by a network policy, so the setup is: network rule and policy for the Connect
  cluster's egress addresses, service user, role, then
  `ALTER USER <user> ADD PROGRAMMATIC ACCESS TOKEN <name> ROLE_RESTRICTION = '<role>'` — whose
  output shows the secret once. Tokens expire after 15 days by default and 365 days at most, so
  plan the rotation.
- **A glibc-based runtime image.** The SDK ships a Rust core loaded over JNI and supports
  x86\_64/ARM64 Linux with glibc ≥ 2.26, macOS ARM64 and Windows. **Alpine/musl images will not
  work.** The native libraries also add roughly 100 MB to the shaded jar.
- **Java 17+**, as before.

### Delivery semantics

Each topic-partition gets its own channel, named `<channel_name_prefix>_<topic>_<partition>`, and
the Kafka offset is used as the channel's offset token. On `open()` the connector reads the
channel's committed token and rewinds the consumer to it, so a restart resumes exactly where
Snowflake left off rather than where Kafka thinks it was. `preCommit` reports the committed token
rather than what was merely handed to `put()`, so Kafka offsets only advance once the data is
durable in Snowflake.

Unlike v3, v4 does **not** compact the batch by primary key in memory: every event is streamed.
Tables with many repeated updates to the same key will therefore ingest more bytes than v3 wrote,
which matters because billing is per ingested byte. Measure a real topic before migrating
everything.

### Record formats

Two formats are read, and **neither has to be configured**: the connector works out which one a
record is, per record. Both land in the same ingest table with the same `IH_*` metadata, so
everything downstream — MERGE, cleanup job, Dynamic Table — is unchanged.

The detection is not a guess. A Debezium envelope always arrives as a `Struct` whose schema carries
`op` next to `before`/`after`; no converter produces that by accident. Anything else — a `Map` from
`JsonConverter` with `schemas.enable=false`, JSON text, JSON bytes, a flat `Struct` — is the row
itself, and then the operation comes from the header. `profile` pins it to `cdc_schema` or
`flat_json` when you would rather reject anything that is not the expected shape.

**Debezium envelope** (`cdc_schema`) — as produced by the Avro converter or by the JSON converter
with a schema registry. The operation comes from the envelope's `op` field, the row from `after`
(`before` on a delete), and the primary key from the record's key schema.

**Flat JSON** (`flat_json`, since 4.0.4) — the row itself as a flat JSON object in the value, and
the operation in a Kafka header. This is the format described in
[docs.inthub.io/outputs/snowflake](https://docs.inthub.io/outputs/snowflake):

| Part | Content |
|---|---|
| header `op` | `r` or `c` (insert), `u` (update), `d` (delete) |
| header `schema`, `table` | Source schema and table |
| value | The whole row as flat JSON. Empty on a delete |
| key | The key columns as JSON. Required on a delete |

```
Key:     {"ID":42}
Value:   {"ID":42,"CLIENTE":"ACME","TOTAL":249.90,"ATUALIZADO_EM":"2026-07-26T11:02:31Z"}
Headers: op=u  schema=dbo  table=pedidos
```

The only thing this format needs in the connector definition is the converter — no `profile`:

```yaml
key.converter: "org.apache.kafka.connect.json.JsonConverter"
key.converter.schemas.enable: "false"
value.converter: "org.apache.kafka.connect.json.JsonConverter"
value.converter.schemas.enable: "false"
```

- Field names are matched to the ingest columns case-insensitively. A field with no column is
  dropped; a column with no field lands as NULL.
- The value may arrive as a `Map` (`JsonConverter` with `schemas.enable=false`, the usual setup), a
  `Struct`, JSON text (`StringConverter`) or JSON bytes (`ByteArrayConverter`). Nested objects and
  arrays are written as JSON text rather than as a Java `toString()`.
- The `op` header is matched case-insensitively and normalized to lower case, because the MERGE
  compares `ih_op` against lower case literals and Snowflake string comparison is case sensitive.
  `op_header` renames the header, for producers that send `__op`.
- **Merge mode needs a primary key.** It comes from the key JSON, so a topic whose records carry no
  key has to name it with `pk_fields`; the task fails with that message rather than merging on
  nothing. `ingestion_only` needs no key at all.
- `schema` and `table` say where the row came from. This connector still writes to the one table
  `table` names — one connector, one table — so they are only recorded, into the `IH_SCHEMA` and
  `IH_TABLE` ingest columns when those exist.
- A temporal column sent as an ISO string passes straight through for Snowflake to parse.
  `timestamp_fields_convert` and friends still convert when the value is a number, so the same
  connector definition works for both shapes.

### Configuration

| Key | Type | Default | Description |
|---|---|---|---|
| `url` | string | — | JDBC URL; also the source of the account, database and endpoint |
| `user` | string | — | Snowflake user owning the PAT |
| `password` | password | — | The PAT, same key and same place as v2/v3 |
| `schema` | string | URL's `schema` | Target schema |
| `table` | string | — | Final table; rows are streamed into `<table>_INGEST` |
| `role` | string | user's default | Streaming session only, never sent over JDBC |
| `streaming_url` | string | `https://<host of url>:443` | Only when the streaming endpoint differs from the JDBC one |
| `ingestion_only` | boolean | `false` | Stream into `_INGEST` only: no MERGE, no cleanup job |
| `profile` | string | `auto` | Record format. `auto` detects it per record; `cdc_schema` or `flat_json` pin it |
| `op_header` | string | `op` | Header carrying the operation; `flat_json` only |
| `pk_fields` | list | empty | Primary key columns. Empty means the record key names them |
| `pipe` | string | `<table>_INGEST-STREAMING` | Override to use a custom pipe |
| `channel_name_prefix` | string | connector name | Must be stable across restarts |
| `max_client_lag_seconds` | int | SDK default | Higher buffers longer, writing fewer and larger files |
| `merge_interval` | duration | `PT0S` | Minimum time between MERGE cycles; merge mode only |
| `append_max_retries` | int | `5` | Retries with backoff on 408/429/500/503 |
| `fail_on_row_error` | boolean | `true` | Fail the task when the pipe rejects rows, as v3's COPY did |
| `find_columns_in_metadata` | boolean | `false` | Same meaning and default as v3 |
| `ignore_columns` | list | empty | Ingest-table columns never written to |
| `exclude_ingest_additional_fields` | list | the `IH_*` columns | Ingest columns absent from the final table. Names the ingest table does not have are ignored |
| `timestamp_fields_convert` | list | empty | Columns whose epoch-millis value becomes a `LocalDateTime` |
| `date_fields_convert` | list | empty | Columns whose epoch-days value becomes a `LocalDate` |
| `time_fields_convert` | list | empty | Columns whose nanos-of-day value becomes a `LocalTime` |
| `job_cleanup_duration` | duration | `PT4H` | Cleanup job interval; merge mode only |
| `job_cleanup_retention_hours` | int | `4` | Hours of rows the cleanup job keeps |
| `job_cleanup_disable` | boolean | `false` | Always disabled when `ingestion_only` is true |

The v3 keys `stage`, `tmp_data_folder`, `buffer_initial_capacity` and `copy_only` do not exist in
v4, nor do the ones that were already dead code in v3 (`pk`, `always_truncate_before_bulk`,
`truncate_when_nodata_after_seconds`, `redis_*`).

Nothing was added to the connection either — it keeps v3's shape exactly: only `user` and `password` are
handed to the driver, and the account, database, schema and role come from the URL and the user's
defaults. The streaming SDK cannot work that way — it takes the account, endpoint, database and
schema as explicit arguments — so the connector derives them from that same JDBC URL:

| The SDK needs | v4 reads it from |
|---|---|
| account | the first host label, or an explicit `account=` parameter |
| endpoint | `https://<host>:443`, so PrivateLink and region-qualified hosts just work |
| database | the `db=` (or `database=`) parameter |
| schema | the `schema` config, falling back to the `schema=` parameter |

So a v3 URL works unchanged.

The temporal conversions keep v3's behaviour of using the JVM's default timezone, so a v3 → v4
migration does not shift existing values. Set `TZ` explicitly on the Connect pods if that matters
to you.

### The tables

Rows are streamed into `<table>_INGEST`, which mirrors the source columns and adds the `IH_*`
metadata the deduplication downstream orders by:

```sql
CREATE TABLE IF NOT EXISTS EVENTS_INGEST (
    -- business columns, must mirror the source table
    ID    NUMBER NOT NULL,
    TYPE  VARCHAR(50),

    -- connector metadata
    IH_TOPIC      VARCHAR(500),
    IH_PARTITION  NUMBER,
    IH_OFFSET     NUMBER,
    IH_OP         VARCHAR(1),     -- operation: c, r, u, d
    IH_DATETIME   TIMESTAMP_NTZ,
    IH_BLOCKID    VARCHAR(36)     -- only used when ingestion_only = false
);
```

The final table `EVENTS` only exists when `ingestion_only` is false; otherwise its place is taken
by the Dynamic Table below. The connector writes only the columns the ingest table actually has:
a field with no column is dropped, a column with no field lands as NULL.

### Example connector definition

```json
{
  "connector.class": "br.com.datastreambrasil.v4.SnowflakeSinkConnector",
  "tasks.max": "6",
  "topics": "cdc.test.dbo.events",

  "url": "jdbc:snowflake://<account>.snowflakecomputing.com?db=<DB>&schema=<SCHEMA>&CLIENT_SESSION_KEEP_ALIVE=TRUE",
  "user": "SVC_KCONNECT",
  "password": "${secrets:kafka/snowflake-pat:token}",
  "schema": "<SCHEMA>",
  "table": "EVENTS",

  "ingestion_only": "true",
  "timestamp_fields_convert": "DTA_INICIO,DTA_FIM",

  "key.converter": "io.confluent.connect.avro.AvroConverter",
  "key.converter.schema.registry.url": "http://schema-registry:8081",
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "http://schema-registry:8081"
}
```

For flat JSON, the same definition with the JSON converters instead — and nothing else, because the
format is detected rather than configured:

```json
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false"
```

`tasks.max` should match the topic's partition count, and `timestamp_fields_convert` is mandatory
for every temporal column that arrives as an epoch number.

### Deduplicating with a Dynamic Table

With `ingestion_only: true` the ingest table is an event log: every version of every key, in
arrival order. What turns it into current state is a Dynamic Table:

```sql
CREATE OR REPLACE DYNAMIC TABLE EVENTS
    TARGET_LAG = '5 minutes'
    WAREHOUSE  = <WAREHOUSE>
AS
SELECT * EXCLUDE (IH_TOPIC, IH_PARTITION, IH_OFFSET, IH_OP, IH_DATETIME, IH_BLOCKID)
FROM (
    SELECT *
    FROM EVENTS_INGEST
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ID ORDER BY IH_OFFSET DESC) = 1
)
WHERE IH_OP <> 'd';
```

Three things are deliberate there. The `QUALIFY` runs **before** the operation filter, which is
what stops an older update from resurrecting a key whose last event was a delete. The `ORDER BY
IH_OFFSET` assumes a given primary key always lands in the same Kafka partition, which holds for
topics keyed by primary key. And the `EXCLUDE` list has to name exactly the `IH_*` columns your
ingest table has — Snowflake rejects the whole statement over one that does not exist.

Note that with `ingestion_only: true` the connector never deletes from the ingest table: the newest
row per key *is* the current state, so a blanket "delete rows older than N hours" would drop live
records. Trim only superseded versions.

### Deploying on Strimzi: raise the /tmp size limit

The SDK extracts its ~29 MiB Rust core into `java.io.tmpdir` and `dlopen()`s it from there.
Strimzi mounts `/tmp` as a **memory-backed `EmptyDir` that defaults to `5Mi`**, so the extraction
fails with `No space left on device` and the task dies with the unhelpful
`Failed to load both main and test libraries`:

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnect
spec:
  template:
    pod:
      tmpDirSizeLimit: 128Mi
```

It is pod memory rather than disk, so keep it just big enough. If you would rather not spend pod
memory, mount a disk-backed volume and point the JVM at it instead:

```yaml
spec:
  jvmOptions:
    javaSystemProperties:
      - name: java.io.tmpdir
        value: /tmp/native
  template:
    pod:
      volumes:
        - name: native-tmp
          emptyDir:
            sizeLimit: 256Mi
    connectContainer:
      volumeMounts:
        - name: native-tmp
          mountPath: /tmp/native
```

From v4.0.2 the connector detects this and says so, instead of surfacing the SDK's message. Note
that the JVM caches a failed static initializer, so the SDK's own loader error is only logged the
first time it is touched in a worker — a restarted task never sees it again.
