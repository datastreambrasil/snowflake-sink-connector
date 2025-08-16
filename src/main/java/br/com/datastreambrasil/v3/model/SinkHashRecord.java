package br.com.datastreambrasil.v3.model;

/**
 * Represents a record containing hash information for a sink operation.
 * firstPreviousHash: The first hash seen for the record, which is used to identify the initial state.
 * previousHash: The hash of the record before the current one.
 * newHash: The hash of the current record being processed.
 */
public record SinkHashRecord(String firstPreviousHash, String previousHash, String newHash) {
}
