package br.com.datastreambrasil.v4;

import br.com.datastreambrasil.v4.exception.InvalidStructException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The {@code cdc_schema} profile: a Debezium envelope carrying a schema, as produced by the
 * Avro or JSON converter with a schema registry.
 *
 * <p>The operation comes from the envelope's {@code op} field, the row from {@code after}
 * ({@code before} for a delete), and the primary key from the record's key schema.
 */
public class CdcSchemaRowMapper extends RowMapper {

    private static final Logger LOGGER = LogManager.getLogger(CdcSchemaRowMapper.class);

    protected static final String AFTER = "after";
    protected static final String BEFORE = "before";
    protected static final String OP = "op";

    public CdcSchemaRowMapper(List<String> ingestColumns,
                              List<String> pkFields,
                              List<String> timestampFieldsConvert,
                              List<String> dateFieldsConvert,
                              List<String> timeFieldsConvert) {
        super(ingestColumns, pkFields, timestampFieldsConvert, dateFieldsConvert, timeFieldsConvert);
    }

    /** Debezium operation carried by the record. Throws when the envelope is malformed. */
    @Override
    public String operationOf(SinkRecord record) {
        var fieldOP = record.valueSchema().field(OP);
        if (fieldOP == null) {
            LOGGER.error("Field '{}' not found in value schema for record: {}", OP, record);
            throw new InvalidStructException("Field '" + OP + "' not found in value schema");
        }

        var valueOP = ((Struct) record.value()).getString(fieldOP.name());
        if (valueOP == null) {
            LOGGER.error("Value for field '{}' is null in record: {}", OP, record);
            throw new InvalidStructException("Value for field '" + OP + "' is null");
        }
        return valueOP;
    }

    /** Primary key column names, taken from the record's key schema like v3 did. */
    @Override
    protected List<String> pkFromRecord(SinkRecord record) {
        var pks = new ArrayList<String>();
        for (Field field : record.keySchema().fields()) {
            pks.add(field.name());
        }
        if (pks.isEmpty()) {
            throw new InvalidStructException("Record key schema has no fields, cannot derive primary key");
        }
        return List.copyOf(pks);
    }

    @Override
    public void validate(SinkRecord record) {
        if (record.keySchema() == null || record.valueSchema() == null
                || !(record.key() instanceof Struct) || !(record.value() instanceof Struct)) {
            LOGGER.error("Key and value must be Structs with schemas. Key: {}, Value: {}",
                    record.key(), record.value());
            throw new InvalidStructException("Invalid record structure or schema");
        }

        if (record.topic() == null || record.kafkaPartition() == null) {
            LOGGER.error("Null values for topic or kafkaPartition. Topic {}, KafkaPartition {}",
                    record.topic(), record.kafkaPartition());
            throw new InvalidStructException("Invalid record structure or schema");
        }
    }

    @Override
    protected Map<String, Object> payloadFields(SinkRecord record, String op) {
        var isDelete = Operation.d.name().equalsIgnoreCase(op);
        var payload = isDelete
                ? ((Struct) record.value()).getStruct(BEFORE)
                : ((Struct) record.value()).getStruct(AFTER);

        if (payload == null) {
            LOGGER.error("Record has no '{}' payload for operation '{}': {}",
                    isDelete ? BEFORE : AFTER, op, record);
            throw new InvalidStructException("Missing payload struct for operation '" + op + "'");
        }

        // case-insensitive lookup of the Debezium fields, mirroring v3's equalsIgnoreCase match
        var fieldsByName = new HashMap<String, Object>();
        for (Field field : payload.schema().fields()) {
            fieldsByName.put(field.name().toUpperCase(Locale.ROOT), payload.get(field.name()));
        }
        return fieldsByName;
    }
}
