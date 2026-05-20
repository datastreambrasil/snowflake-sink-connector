package br.com.datastreambrasil.v3.compress;

import br.com.datastreambrasil.v3.compress.serializer.ByteBufferSerializer;
import br.com.datastreambrasil.v3.compress.serializer.LocalDateTimeSerializer;
import br.com.datastreambrasil.v3.model.FieldRecord;
import br.com.datastreambrasil.v3.model.SnowflakeRecord;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.Pool;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;

public class KryoFactory {

    private final Pool<Kryo> pool;

    public KryoFactory() {
        this.pool = new Pool<>(true, false, 16) {
            @Override
            protected Kryo create() {
                Kryo kryo = new Kryo();
                kryo.setRegistrationRequired(false);
                kryo.setReferences(false); // desativa referências cruzadas → mais rápido

                // registrar classes frequentes para eliminar overhead de nome completo
                kryo.register(SnowflakeRecord.class, 10);
                kryo.register(LocalDateTime.class, new LocalDateTimeSerializer(), 11);
                kryo.register(FieldRecord.class, 12);
                kryo.register(ByteBuffer.class, new ByteBufferSerializer(), 13);

                return kryo;
            }
        };
    }

    public <T> byte[] serialize(T object) {
        Kryo kryo = pool.obtain();
        try (Output output = new Output(256, -1)) {
            kryo.writeClassAndObject(output, object);
            return output.toBytes();
        } finally {
            pool.free(kryo);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T deserialize(byte[] bytes) {
        Kryo kryo = pool.obtain();
        try (Input input = new Input(bytes)) {
            return (T) kryo.readClassAndObject(input);
        } finally {
            pool.free(kryo);
        }
    }
}
