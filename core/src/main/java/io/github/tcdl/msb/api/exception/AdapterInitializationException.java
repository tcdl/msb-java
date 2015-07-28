package io.github.tcdl.msb.api.exception;

/**
 * Exception is thrown in case problem occurred during creation of Adapter Factory object.
 */
public class AdapterInitializationException extends MsbException{
    public AdapterInitializationException(String message) {
        super(message);
    }

    public AdapterInitializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
