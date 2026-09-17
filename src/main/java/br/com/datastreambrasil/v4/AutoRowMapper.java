package br.com.datastreambrasil.v4;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * The default profile: works out per record whether it is a Debezium envelope or a flat JSON
 * row, so neither format has to be configured.
 *
 * <p>The two shapes are not ambiguous. A Debezium envelope always arrives as a {@link Struct}
 * whose schema carries {@code op} next to {@code before}/{@code after} - that is the envelope,
 * and no converter produces it by accident. Anything else - a {@code Map} from
 * {@code JsonConverter} with {@code schemas.enable=false}, JSON text, JSON bytes, a flat
 * {@code Struct} - is the row itself, and then the operation comes from the header.
 *
 * <p>The decision is per record rather than per connector, so a topic that switches format
 * mid-stream keeps working. Set {@code profile} explicitly to turn the detection off and reject
 * anything that is not the expected shape.
 */
public class AutoRowMapper extends RowMapper {

    private static final Logger LOGGER = LogManager.getLogger(AutoRowMapper.class);

    private final CdcSchemaRowMapper cdc;
    private final FlatJsonRowMapper flat;

    /** Last format logged, so the detection says what it found without repeating itself. */
    private String reported;

    public AutoRowMapper(List<String> ingestColumns,
                         List<String> pkFields,
                         List<String> timestampFieldsConvert,
                         List<String> dateFieldsConvert,
                         List<String> timeFieldsConvert,
                         String opHeader) {
        super(ingestColumns, pkFields, timestampFieldsConvert, dateFieldsConvert, timeFieldsConvert);
        this.cdc = new CdcSchemaRowMapper(ingestColumns, pkFields, timestampFieldsConvert,
                dateFieldsConvert, timeFieldsConvert);
        this.flat = new FlatJsonRowMapper(ingestColumns, pkFields, timestampFieldsConvert,
                dateFieldsConvert, timeFieldsConvert, opHeader);
    }

    /** Which mapper this record belongs to. */
    protected RowMapper mapperFor(SinkRecord record) {
        var schema = record.valueSchema();
        var isEnvelope = record.value() instanceof Struct
                && schema != null
                && schema.field(CdcSchemaRowMapper.OP) != null
                && (schema.field(CdcSchemaRowMapper.AFTER) != null
                    || schema.field(CdcSchemaRowMapper.BEFORE) != null);

        RowMapper picked = isEnvelope ? cdc : flat;
        report(isEnvelope ? SnowflakeSinkConnector.PROFILE_CDC_SCHEMA
                : SnowflakeSinkConnector.PROFILE_FLAT_JSON);
        return picked;
    }

    private void report(String profile) {
        // only touched from the Connect worker thread that calls put()
        if (!profile.equals(reported)) {
            LOGGER.info("Record format detected: {}", profile);
            reported = profile;
        }
    }

    @Override
    public String operationOf(SinkRecord record) {
        return mapperFor(record).operationOf(record);
    }

    @Override
    public void validate(SinkRecord record) {
        mapperFor(record).validate(record);
    }

    @Override
    public Map<String, Object> toRow(SinkRecord record, String blockId) {
        return mapperFor(record).toRow(record, blockId);
    }

    @Override
    protected Map<String, Object> payloadFields(SinkRecord record, String op) {
        return mapperFor(record).payloadFields(record, op);
    }

    @Override
    protected List<String> pkFromRecord(SinkRecord record) {
        return mapperFor(record).pkFromRecord(record);
    }
}
