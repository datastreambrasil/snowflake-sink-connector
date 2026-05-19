package br.com.datastreambrasil.v3.compress.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.time.LocalDateTime;

public class LocalDateTimeSerializer extends Serializer<LocalDateTime> {

    @Override
    public void write(Kryo kryo, Output output, LocalDateTime dateTime) {
        output.writeInt(dateTime.getYear());
        output.writeByte(dateTime.getMonthValue());
        output.writeByte(dateTime.getDayOfMonth());
        output.writeByte(dateTime.getHour());
        output.writeByte(dateTime.getMinute());
        output.writeByte(dateTime.getSecond());
        output.writeInt(dateTime.getNano());
    }

    @Override
    public LocalDateTime read(Kryo kryo, Input input, Class<? extends LocalDateTime> type) {
        int year   = input.readInt();
        int month  = input.readByte();
        int day    = input.readByte();
        int hour   = input.readByte();
        int minute = input.readByte();
        int second = input.readByte();
        int nano   = input.readInt();
        return LocalDateTime.of(year, month, day, hour, minute, second, nano);
    }
}