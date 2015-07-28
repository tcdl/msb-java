package io.github.tcdl.msb.api.exception;

/**
 * Exception is thrown in case problem occurred during communication with broker provider.
 */
public class ChannelException extends MsbException {

    public ChannelException(String message, Throwable cause) {
        super(message, cause);
    }
}
