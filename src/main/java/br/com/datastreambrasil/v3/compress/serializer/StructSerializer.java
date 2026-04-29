package br.com.datastreambrasil.v3.compress.serializer;

import br.com.datastreambrasil.v3.compress.writer.FieldValueWriter;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public class StructSerializer extends Serializer<Struct> {

    private final SchemaSerializer schemaSerializer = new SchemaSerializer();
    private final FieldValueWriter valueWriter = new FieldValueWriter(schemaSerializer);

    @Override
    public void write(Kryo kryo, Output output, Struct struct) {
        Schema schema = struct.schema();

        // 1. serializa o schema completo
        schemaSerializer.write(kryo, output, schema);

        // 2. serializa os valores na ordem dos campos do schema
        for (Field field : schema.fields()) {
            valueWriter.writeValue(output, field.schema(), struct.get(field));
        }
    }

    @Override
    public Struct read(Kryo kryo, Input input, Class<? extends Struct> type) {
        // 1. reconstrói o schema
        Schema schema = schemaSerializer.read(kryo, input, Schema.class);

        // 2. reconstrói o Struct populando campo a campo
        Struct struct = new Struct(schema);
        for (Field field : schema.fields()) {
            Object value = valueWriter.readValue(input, field.schema());
            struct.put(field.name(), value);
        }
        return struct;
    }
}
