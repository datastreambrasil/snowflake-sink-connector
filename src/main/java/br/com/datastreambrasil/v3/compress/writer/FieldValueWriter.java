package br.com.datastreambrasil.v3.compress.writer;
// serializer/connect/FieldValueWriter.java
import br.com.datastreambrasil.v3.compress.serializer.SchemaSerializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public final class FieldValueWriter {

    private final SchemaSerializer schemaSerializer;

    public FieldValueWriter(SchemaSerializer schemaSerializer) {
        this.schemaSerializer = schemaSerializer;
    }

    @SuppressWarnings("unchecked")
    public void writeValue(Output output, Schema schema, Object value) {
        // null handling — válido para campos opcionais
        if (value == null) {
            output.writeBoolean(true);
            return;
        }
        output.writeBoolean(false);

        switch (schema.type()) {
            case INT8    -> output.writeByte((Byte) value);
            case INT16   -> output.writeShort((Short) value);
            case INT32   -> output.writeInt((Integer) value);
            case INT64   -> output.writeLong((Long) value);
            case FLOAT32 -> output.writeFloat((Float) value);
            case FLOAT64 -> output.writeDouble((Double) value);
            case BOOLEAN -> output.writeBoolean((Boolean) value);
            case STRING  -> output.writeString((String) value);

            case BYTES -> {
                byte[] bytes = value instanceof ByteBuffer bb
                        ? bb.array()
                        : (byte[]) value;
                output.writeInt(bytes.length);
                output.writeBytes(bytes);
            }

            case STRUCT -> {
                // recursão: Struct aninhado
                Struct nested = (Struct) value;
                for (var field : schema.fields()) {
                    writeValue(output, field.schema(), nested.get(field));
                }
            }

            case ARRAY -> {
                List<Object> list = (List<Object>) value;
                output.writeInt(list.size());
                for (Object item : list) {
                    writeValue(output, schema.valueSchema(), item);
                }
            }

            case MAP -> {
                Map<Object, Object> map = (Map<Object, Object>) value;
                output.writeInt(map.size());
                map.forEach((k, v) -> {
                    writeValue(output, schema.keySchema(), k);
                    writeValue(output, schema.valueSchema(), v);
                });
            }

            default -> throw new IllegalArgumentException(
                    "Tipo não suportado: " + schema.type());
        }
    }

    public Object readValue(Input input, Schema schema) {
        boolean isNull = input.readBoolean();
        if (isNull) return null;

        return switch (schema.type()) {
            case INT8    -> input.readByte();
            case INT16   -> input.readShort();
            case INT32   -> input.readInt();
            case INT64   -> input.readLong();
            case FLOAT32 -> input.readFloat();
            case FLOAT64 -> input.readDouble();
            case BOOLEAN -> input.readBoolean();
            case STRING  -> input.readString();

            case BYTES -> {
                int len   = input.readInt();
                yield input.readBytes(len);
            }

            case STRUCT -> {
                Struct nested = new Struct(schema);
                for (var field : schema.fields()) {
                    nested.put(field.name(), readValue(input, field.schema()));
                }
                yield nested;
            }

            case ARRAY -> {
                int size = input.readInt();
                List<Object> list = new java.util.ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    list.add(readValue(input, schema.valueSchema()));
                }
                yield list;
            }

            case MAP -> {
                int size = input.readInt();
                Map<Object, Object> map = new java.util.LinkedHashMap<>(size);
                for (int i = 0; i < size; i++) {
                    Object k = readValue(input, schema.keySchema());
                    Object v = readValue(input, schema.valueSchema());
                    map.put(k, v);
                }
                yield map;
            }

            default -> throw new IllegalArgumentException(
                    "Tipo não suportado: " + schema.type());
        };
    }
}
