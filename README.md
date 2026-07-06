## Sink connector to write data to snowflake

This connector is used to write data to snowflake. It is a sink connector that reads data from Kafka topics and writes it to snowflake tables.

### MERGE deduplication via HASH

When flushing buffered records into the final Snowflake table, the connector uses a `MERGE` statement with a **HASH-based guard** on the `WHEN MATCHED` clause:

```sql
WHEN MATCHED AND HASH(final.col1, final.col2, ...) <> HASH(ingest.col1, ingest.col2, ...) THEN UPDATE SET ...
```

This prevents Snowflake from writing a new micro-partition when a full-load re-publish sends records that are **identical** to what is already stored. The `UPDATE` only executes when the hash of the incoming row differs from the hash of the existing row, eliminating unnecessary DML and the micro-partition fragmentation it causes.

This behaviour covers all columns present in the final table (`columnsFinalTable`), including both PK and non-PK fields.
