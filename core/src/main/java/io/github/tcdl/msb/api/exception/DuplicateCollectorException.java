package io.github.tcdl.msb.api.exception;

/**
 * Exception is thrown in case more then one Collector is created per correlationId.
 */
public class DuplicateCollectorException extends MsbException {
    public DuplicateCollectorException(String message) {
        super(message);
    }
}
