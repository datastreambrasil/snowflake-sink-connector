package br.com.datastreambrasil.v3.model;

import java.io.Serializable;

public record FieldRecord(String name, Object data) implements Serializable {
}
