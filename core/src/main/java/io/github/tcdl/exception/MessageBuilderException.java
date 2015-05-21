package io.github.tcdl.exception;

/**
 * Wraps any exception thrown while converting to/from json/object during building message objects
 *
 * Created by rdro on 5/21/2015.
 */
public class MessageBuilderException extends RuntimeException {

    public MessageBuilderException(String message) {
        super(message);
    }
}
