package br.com.datastreambrasil.v3.model;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;

public record SnowflakeRecord(List<FieldRecord> event, String topic, int partition, long offset, String op,
                              LocalDateTime timestamp) implements Serializable {
}
