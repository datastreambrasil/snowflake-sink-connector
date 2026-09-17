package br.com.datastreambrasil.v4;

import br.com.datastreambrasil.v4.exception.InvalidStructException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The {@code flat_json} profile: the row itself in the value as a flat JSON object, and the
 * operation in a Kafka header.
 *
 * <pre>
 *   Key:     {"ID":42}
 *   Value:   {"ID":42,"CLIENTE":"ACME","TOTAL":249.90}
 *   Headers: op=u  schema=dbo  table=pedidos
 * </pre>
 *
 * <ul>
 *   <li>{@code op} is {@code r} or {@code c} (insert), {@code u} (update) or {@code d} (delete),
 *       the same four codes the Debezium profile uses, so the MERGE downstream is unchanged.</li>
 *   <li>A delete carries no value: the row it names comes from the key, which is also where the
 *       primary key columns are read from when {@code pk_fields} is not configured.</li>
 *   <li>{@code schema} and {@code table} name the source table. This connector writes to the one
 *       table it was configured with, so they are only stamped into the {@code IH_SCHEMA} and
 *       {@code IH_TABLE} ingest columns when those exist.</li>
 * </ul>
 *
 * <p>The value may arrive already deserialized as a {@code Map} (the usual case, with
 * {@code JsonConverter} and {@code schemas.enable=false}) or as a {@code Struct}, a JSON
 * {@code String} or JSON bytes, so {@code StringConverter} and {@code ByteArrayConverter} work
 * too.
 */
public class FlatJsonRowMapper extends RowMapper {

    private static final Logger LOGGER = LogManager.getLogger(FlatJsonRowMapper.class);

    /** Headers naming the source table, per the flat JSON contract. */
    protected static final String HEADER_SCHEMA = "schema";
    protected static final String HEADER_TABLE = "table";

    // BigDecimal rather than double for JSON floats: a NUMBER(18,2) column must not pick up a
    // binary rounding error on the way in.
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private final String opHeader;

    public FlatJsonRowMapper(List<String> ingestColumns,
                             List<String> pkFields,
                             List<String> timestampFieldsConvert,
                             List<String> dateFieldsConvert,
                             List<String> timeFieldsConvert,
                             String opHeader) {
        super(ingestColumns, pkFields, timestampFieldsConvert, dateFieldsConvert, timeFieldsConvert);
        this.opHeader = (opHeader == null || opHeader.isBlank()) ? "op" : opHeader.trim();
    }

    /**
     * The operation, read from the header and normalized to lower case - the MERGE compares
     * {@code ih_op} against lower case literals and Snowflake string comparison is case
     * sensitive, so an {@code op=U} must not reach the ingest table as {@code U}.
     */
    @Override
    public String operationOf(SinkRecord record) {
        var raw = header(record, opHeader);
        if (raw == null || raw.isBlank()) {
            LOGGER.error("Header '{}' not found in record: {}", opHeader, record);
            throw new InvalidStructException("Header '" + opHeader + "' not found; the flat JSON "
                    + "profile takes the operation from that header, with one of r, c, u or d");
        }

        var op = raw.trim().toLowerCase(Locale.ROOT);
        try {
            Operation.valueOf(op);
        } catch (IllegalArgumentException e) {
            LOGGER.error("Unknown operation '{}' in header '{}' of record: {}", raw, opHeader, record);
            throw new InvalidStructException("Unknown operation '" + raw + "' in header '"
                    + opHeader + "'; expected one of r, c, u or d");
        }
        return op;
    }

    @Override
    public void validate(SinkRecord record) {
        if (record.topic() == null || record.kafkaPartition() == null) {
            LOGGER.error("Null values for topic or kafkaPartition. Topic {}, KafkaPartition {}",
                    record.topic(), record.kafkaPartition());
            throw new InvalidStructException("Invalid record structure");
        }
    }

    @Override
    protected Map<String, Object> payloadFields(SinkRecord record, String op) {
        if (Operation.d.name().equals(op)) {
            // a delete carries no value: the key names the row to remove. A producer that does
            // send the whole row on a delete still works, the key simply wins.
            if (record.key() != null) {
                return fieldsOf(record.key(), "key");
            }
            if (record.value() != null) {
                return fieldsOf(record.value(), "value");
            }
            LOGGER.error("Delete record has neither key nor value: {}", record);
            throw new InvalidStructException("Delete record has no key; a delete names the row "
                    + "to remove by its key columns in JSON");
        }

        if (record.value() == null) {
            LOGGER.error("Record value is null for operation '{}': {}", op, record);
            throw new InvalidStructException("Record value is null for operation '" + op
                    + "'; only a delete may have an empty value");
        }
        return fieldsOf(record.value(), "value");
    }

    /**
     * The key columns, in the order the key JSON lists them. Comes back empty when the record
     * has no key, which is when {@code pk_fields} has to say what the primary key is.
     */
    @Override
    protected List<String> pkFromRecord(SinkRecord record) {
        if (record.key() == null) {
            return List.of();
        }
        var key = fieldsOf(record.key(), "key");
        if (key.isEmpty()) {
            return List.of();
        }
        return List.copyOf(key.keySet());
    }

    /** Adds the source schema and table from the headers, for ingest tables that have them. */
    @Override
    protected Map<String, Object> metadata(SinkRecord record, String op, String blockId) {
        var metadata = super.metadata(record, op, blockId);

        var sourceSchema = header(record, HEADER_SCHEMA);
        if (sourceSchema != null) {
            metadata.put(IHSCHEMA, sourceSchema);
        }
        var sourceTable = header(record, HEADER_TABLE);
        if (sourceTable != null) {
            metadata.put(IHTABLE, sourceTable);
        }
        return metadata;
    }

    /** A nested object or array becomes JSON text, not a Java {@code toString()}. */
    @Override
    protected Object convert(String column, Object value) {
        if (value instanceof Map || value instanceof Collection) {
            try {
                return JSON.writeValueAsString(value);
            } catch (JsonProcessingException e) {
                LOGGER.warn("Could not serialize the nested value of column {}, falling back to "
                        + "its string form", column, e);
                return value.toString();
            }
        }
        return super.convert(column, value);
    }

    /**
     * Reads a header by name, falling back to a case-insensitive match so a producer that sends
     * {@code OP} instead of {@code op} is not silently rejected.
     */
    private static String header(SinkRecord record, String name) {
        var headers = record.headers();
        if (headers == null) {
            return null;
        }

        var header = headers.lastWithName(name);
        if (header == null) {
            for (Header candidate : headers) {
                if (candidate.key() != null && candidate.key().equalsIgnoreCase(name)) {
                    header = candidate;
                }
            }
        }
        return header == null ? null : asText(header.value());
    }

    private static String asText(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String text) {
            return text;
        }
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        if (value instanceof ByteBuffer buffer) {
            return StandardCharsets.UTF_8.decode(buffer.duplicate()).toString();
        }
        return String.valueOf(value);
    }

    /** Whatever the converter produced, as a map keyed by upper case field name. */
    private Map<String, Object> fieldsOf(Object payload, String what) {
        if (payload == null) {
            return Map.of();
        }
        if (payload instanceof Map<?, ?> map) {
            var fields = new LinkedHashMap<String, Object>(map.size());
            map.forEach((name, value) -> {
                if (name != null) {
                    fields.put(name.toString().toUpperCase(Locale.ROOT), value);
                }
            });
            return fields;
        }
        if (payload instanceof Struct struct) {
            var fields = new LinkedHashMap<String, Object>();
            for (Field field : struct.schema().fields()) {
                fields.put(field.name().toUpperCase(Locale.ROOT), struct.get(field.name()));
            }
            return fields;
        }
        if (payload instanceof String text) {
            return parse(text, what);
        }
        if (payload instanceof byte[] bytes) {
            return parse(new String(bytes, StandardCharsets.UTF_8), what);
        }
        if (payload instanceof ByteBuffer buffer) {
            return parse(StandardCharsets.UTF_8.decode(buffer.duplicate()).toString(), what);
        }

        LOGGER.error("Record {} is a {}, which the flat JSON profile cannot read", what,
                payload.getClass().getName());
        throw new InvalidStructException("Record " + what + " is a " + payload.getClass().getName()
                + "; the flat JSON profile expects a JSON object. Use JsonConverter with "
                + "schemas.enable=false, StringConverter or ByteArrayConverter.");
    }

    private Map<String, Object> parse(String text, String what) {
        if (text.isBlank()) {
            return Map.of();
        }
        try {
            var parsed = JSON.readValue(text, new TypeReference<Map<String, Object>>() {
            });
            var fields = new LinkedHashMap<String, Object>(parsed.size());
            parsed.forEach((name, value) -> fields.put(name.toUpperCase(Locale.ROOT), value));
            return fields;
        } catch (JsonProcessingException e) {
            LOGGER.error("Record {} is not a flat JSON object: {}", what, text, e);
            throw new InvalidStructException("Record " + what + " is not a flat JSON object: "
                    + e.getOriginalMessage());
        }
    }
}
