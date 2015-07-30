package io.github.tcdl.msb.api.exception;

/**
 * Exception is thrown in case more then one consumer is created per namespace.
 */
public class ConsumerSubscriptionException extends MsbException {
    public ConsumerSubscriptionException(String message) {
        super(message);
    }
}
