package io.github.tcdl.msb.api.exception;

/**
 * Wraps any exception thrown while converting Java object to/from during building {@link io.github.tcdl.msb.api.message.Message}.
 */
public class MessageBuilderException extends MsbException {

    public MessageBuilderException(String message) {
        super(message);
    }
}
