package br.com.datastreambrasil.v3.compress.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.nio.ByteBuffer;

public class ByteBufferSerializer extends Serializer<ByteBuffer> {

    @Override
    public void write(Kryo kryo, Output output, ByteBuffer buffer) {
        // rewind para garantir leitura desde o inicio
        // independente do estado do position atual
        ByteBuffer copy = buffer.duplicate();
        copy.rewind(); // position = 0, limit mantido

        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);

        output.writeInt(bytes.length);
        output.writeBytes(bytes);
    }

    @Override
    public ByteBuffer read(Kryo kryo, Input input, Class<? extends ByteBuffer> type) {
        int length    = input.readInt();
        byte[] bytes  = input.readBytes(length);
        return ByteBuffer.wrap(bytes); // position=0, limit=length
    }
}
