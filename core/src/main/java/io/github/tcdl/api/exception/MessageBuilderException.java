package io.github.tcdl.api.exception;

/**
 * Wraps any exception thrown while converting to/from json/object during building message objects
 *
 * Created by rdro on 5/21/2015.
 */
public class MessageBuilderException extends MsbException {

    public MessageBuilderException(String message) {
        super(message);
    }
}
