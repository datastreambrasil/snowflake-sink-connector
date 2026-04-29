package br.com.datastreambrasil.v3.model;

import org.apache.kafka.connect.data.Struct;

import java.io.Serializable;
import java.time.LocalDateTime;

public record SnowflakeRecord(Struct event, String topic, int partition, long offset, String op,
                              LocalDateTime timestamp) implements Serializable {
}
