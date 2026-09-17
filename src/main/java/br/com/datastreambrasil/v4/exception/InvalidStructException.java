package br.com.datastreambrasil.v4.exception;

/**
 * Exception thrown when a struct is invalid, typically due to missing required fields or incorrect schema.
 */
public class InvalidStructException extends RuntimeException {
    public InvalidStructException(String message) {
        super(message);
    }

}
