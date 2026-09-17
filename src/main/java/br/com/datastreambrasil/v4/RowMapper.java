package br.com.datastreambrasil.v4;

import org.apache.kafka.connect.sink.SinkRecord;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

/**
 * Turns a CDC {@link SinkRecord} into the row map Snowpipe Streaming expects.
 *
 * <p>Unlike v3 - which built a positional CSV line ordered by the ingest table's ordinal
 * positions - the pipe matches by column name, so this produces a
 * {@code Map<columnName, value>}. Keys are the column names exactly as they come back from
 * Snowflake's metadata (upper case for unquoted identifiers), which keeps the mapping correct
 * whether or not the pipe is case sensitive.
 *
 * <p>Where the operation, the primary key and the payload come from is the profile's business:
 * {@link CdcSchemaRowMapper} reads them out of a Debezium envelope, {@link FlatJsonRowMapper}
 * out of a flat JSON value plus Kafka headers. Everything after that - which columns are
 * emitted, how values are converted, what metadata is stamped - is the same for both.
 */
public abstract class RowMapper {

    protected static final String IHTOPIC = "IH_TOPIC";
    protected static final String IHOFFSET = "IH_OFFSET";
    protected static final String IHPARTITION = "IH_PARTITION";
    protected static final String IHOP = "IH_OP";
    protected static final String IHDATETIME = "IH_DATETIME";
    protected static final String IHBLOCKID = "IH_BLOCKID";
    protected static final String IHSCHEMA = "IH_SCHEMA";
    protected static final String IHTABLE = "IH_TABLE";

    /**
     * CDC operation codes. Debezium and the flat JSON profile use the same four: delete, create,
     * update, read (snapshot).
     */
    public enum Operation {
        d, c, u, r
    }

    private final List<String> ingestColumns;
    private final List<String> pkFields;
    private final List<String> timestampFieldsConvert;
    private final List<String> dateFieldsConvert;
    private final List<String> timeFieldsConvert;

    protected RowMapper(List<String> ingestColumns,
                        List<String> pkFields,
                        List<String> timestampFieldsConvert,
                        List<String> dateFieldsConvert,
                        List<String> timeFieldsConvert) {
        this.ingestColumns = List.copyOf(ingestColumns);
        this.pkFields = normalize(pkFields);
        this.timestampFieldsConvert = normalize(timestampFieldsConvert);
        this.dateFieldsConvert = normalize(dateFieldsConvert);
        this.timeFieldsConvert = normalize(timeFieldsConvert);
    }

    protected static List<String> normalize(List<String> values) {
        var out = new ArrayList<String>();
        if (values != null) {
            for (String v : values) {
                if (v != null && !v.isBlank()) {
                    out.add(v.trim().toUpperCase(Locale.ROOT));
                }
            }
        }
        return List.copyOf(out);
    }

    /** The operation the record carries, as one of {@link Operation}. */
    public abstract String operationOf(SinkRecord record);

    /** Rejects a record the profile cannot map, before anything is read out of it. */
    public abstract void validate(SinkRecord record);

    /**
     * The record's payload as a map keyed by upper case field name. Which part of the record
     * that is - a Debezium {@code before}/{@code after} struct, a flat JSON value, the key of a
     * delete - is the profile's business.
     */
    protected abstract Map<String, Object> payloadFields(SinkRecord record, String op);

    /**
     * Primary key columns taken from the record itself. May come back empty when the record
     * cannot name them, which is why {@code pk_fields} exists.
     */
    protected abstract List<String> pkFromRecord(SinkRecord record);

    /**
     * Primary key columns for the MERGE. The configured {@code pk_fields} wins when set, so a
     * topic whose records carry no key can still be merged.
     */
    public final List<String> extractPk(SinkRecord record) {
        return pkFields.isEmpty() ? pkFromRecord(record) : pkFields;
    }

    /**
     * Builds the row for one record. Only columns that exist in the ingest table are emitted;
     * a null or absent field is left out of the map entirely, which lands as NULL.
     */
    public Map<String, Object> toRow(SinkRecord record, String blockId) {
        validate(record);

        var op = operationOf(record);
        var fields = payloadFields(record, op);
        var metadata = metadata(record, op, blockId);

        var row = new LinkedHashMap<String, Object>(ingestColumns.size());
        for (String column : ingestColumns) {
            var upper = column.toUpperCase(Locale.ROOT);
            if (metadata.containsKey(upper)) {
                row.put(column, metadata.get(upper));
                continue;
            }
            var value = fields.get(upper);
            if (value == null) {
                // absent or null: leaving the key out makes the pipe write NULL
                continue;
            }
            row.put(column, convert(column, value));
        }

        return row;
    }

    /**
     * The {@code IH_*} columns, keyed upper case. A profile with more to say about the record
     * adds to this - the flat JSON one stamps the source schema and table when the headers
     * carry them.
     */
    protected Map<String, Object> metadata(SinkRecord record, String op, String blockId) {
        var metadata = new HashMap<String, Object>(8);
        metadata.put(IHBLOCKID, blockId);
        metadata.put(IHOP, op);
        metadata.put(IHTOPIC, record.topic());
        metadata.put(IHDATETIME, LocalDateTime.now(ZoneOffset.UTC).toString());
        metadata.put(IHPARTITION, record.kafkaPartition());
        metadata.put(IHOFFSET, record.kafkaOffset());
        return metadata;
    }

    protected Object convert(String column, Object value) {
        var upper = column.toUpperCase(Locale.ROOT);

        // same semantics as v3: the source sends epoch millis / epoch days / nanos-of-day and the
        // configured column lists say how to read them. Kept on the default JVM timezone so a
        // v3 -> v4 migration does not shift existing values. A value that is not a number - a flat
        // JSON payload may well carry the column as an ISO string - passes through untouched
        // instead of failing the task.
        if (value instanceof Number number) {
            if (timestampFieldsConvert.contains(upper)) {
                return LocalDateTime.ofInstant(Instant.ofEpochMilli(number.longValue()),
                        TimeZone.getDefault().toZoneId()).toString();
            }
            if (dateFieldsConvert.contains(upper)) {
                var daysInSeconds = number.longValue() * 24L * 60L * 60L;
                return LocalDate.ofInstant(Instant.ofEpochSecond(daysInSeconds),
                        TimeZone.getDefault().toZoneId()).toString();
            }
            if (timeFieldsConvert.contains(upper)) {
                return LocalTime.ofNanoOfDay(number.longValue()).toString();
            }
        }

        if (value instanceof ByteBuffer buffer) {
            var bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes);
            return bytes;
        }
        if (value instanceof byte[] || value instanceof Number || value instanceof Boolean
                || value instanceof String || value instanceof BigDecimal) {
            return value;
        }
        if (value instanceof Date date) {
            return LocalDateTime.ofInstant(date.toInstant(), ZoneOffset.UTC).toString();
        }
        if (value instanceof LocalDateTime || value instanceof LocalDate || value instanceof LocalTime) {
            return value.toString();
        }

        // anything else (nested Structs, arrays) keeps v3's behaviour of stringifying
        return value.toString();
    }
}
