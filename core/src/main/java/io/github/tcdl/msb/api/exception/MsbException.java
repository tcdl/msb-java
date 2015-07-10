package io.github.tcdl.msb.api.exception;

/**
 * Created by anstr on 6/22/2015.
 * Base class for all exceptions in msb-java library
 */
public class MsbException extends RuntimeException {
    public MsbException(String message) {
        super(message);
    }

    public MsbException(String message, Throwable cause) {
        super(message, cause);
    }
}
