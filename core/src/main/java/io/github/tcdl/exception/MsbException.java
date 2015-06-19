package io.github.tcdl.exception;

/**
 * Created by anstr on 6/19/2015.
 */
public class MsbException extends RuntimeException {
    public MsbException(String message, Throwable cause) {
        super(message, cause);
    }

    public MsbException(String message) {
        super(message);
    }
}
