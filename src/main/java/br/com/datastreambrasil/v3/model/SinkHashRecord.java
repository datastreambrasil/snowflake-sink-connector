package br.com.datastreambrasil.v3.model;

/**
 * Represents a record containing hash information for a sink operation.
 * firstSeenHash: The first seen hash for the same record in the same transaction batch
 * previousHash: The hash of the record before the current one.
 * newHash: The hash of the current record being processed.
 */
public record SinkHashRecord(String firstSeenHash, String previousHash, String newHash) {
}
