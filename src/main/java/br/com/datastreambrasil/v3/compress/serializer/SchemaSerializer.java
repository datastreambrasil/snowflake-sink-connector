package br.com.datastreambrasil.v3.compress.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.List;
import java.util.Map;

public class SchemaSerializer extends Serializer<Schema> {

    @Override
    public void write(Kryo kryo, Output output, Schema schema) {
        // tipo obrigatório
        output.writeString(schema.type().name());

        // metadados opcionais
        output.writeBoolean(schema.isOptional());
        output.writeString(schema.name());           // null → escrito como null
        output.writeBoolean(schema.version() != null);
        if (schema.version() != null) output.writeInt(schema.version());
        output.writeString(schema.doc());

        // parâmetros (Map<String, String>)
        Map<String, String> params = schema.parameters();
        boolean hasParams = params != null && !params.isEmpty();
        output.writeBoolean(hasParams);
        if (hasParams) {
            output.writeInt(params.size());
            params.forEach((k, v) -> { output.writeString(k); output.writeString(v); });
        }

        // campos filhos — apenas STRUCT possui
        if (schema.type() == Schema.Type.STRUCT) {
            List<Field> fields = schema.fields();
            output.writeInt(fields.size());
            for (Field field : fields) {
                output.writeString(field.name());
                write(kryo, output, field.schema()); // recursão
            }
        }

        // MAP: keySchema + valueSchema
        if (schema.type() == Schema.Type.MAP) {
            write(kryo, output, schema.keySchema());
            write(kryo, output, schema.valueSchema());
        }

        // ARRAY: valueSchema
        if (schema.type() == Schema.Type.ARRAY) {
            write(kryo, output, schema.valueSchema());
        }
    }

    @Override
    public Schema read(Kryo kryo, Input input, Class<? extends Schema> type) {
        Schema.Type schemaType = Schema.Type.valueOf(input.readString());

        boolean optional  = input.readBoolean();
        String  name      = input.readString();
        Integer version   = input.readBoolean() ? input.readInt() : null;
        String  doc       = input.readString();

        SchemaBuilder builder = new SchemaBuilder(schemaType);
        if (optional)       builder.optional();
        if (name != null)   builder.name(name);
        if (version != null) builder.version(version);
        if (doc != null)    builder.doc(doc);

        boolean hasParams = input.readBoolean();
        if (hasParams) {
            int size = input.readInt();
            for (int i = 0; i < size; i++) {
                builder.parameter(input.readString(), input.readString());
            }
        }

        if (schemaType == Schema.Type.STRUCT) {
            int fieldCount = input.readInt();
            for (int i = 0; i < fieldCount; i++) {
                String fieldName   = input.readString();
                Schema fieldSchema = read(kryo, input, Schema.class); // recursão
                builder.field(fieldName, fieldSchema);
            }
        }

        if (schemaType == Schema.Type.MAP) {
            Schema keySchema   = read(kryo, input, Schema.class);
            Schema valueSchema = read(kryo, input, Schema.class);
            return optional
                    ? SchemaBuilder.map(keySchema, valueSchema).optional().build()
                    : SchemaBuilder.map(keySchema, valueSchema).build();
        }

        if (schemaType == Schema.Type.ARRAY) {
            Schema valueSchema = read(kryo, input, Schema.class);
            return optional
                    ? SchemaBuilder.array(valueSchema).optional().build()
                    : SchemaBuilder.array(valueSchema).build();
        }

        return builder.build();
    }
}
