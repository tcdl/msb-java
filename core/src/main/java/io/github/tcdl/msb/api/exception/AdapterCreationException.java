package io.github.tcdl.msb.api.exception;

public class AdapterCreationException extends MsbException {
    public AdapterCreationException(String message) {
        super(message);
    }

    public AdapterCreationException(String message, Throwable cause) {
        super(message, cause);
    }
}
