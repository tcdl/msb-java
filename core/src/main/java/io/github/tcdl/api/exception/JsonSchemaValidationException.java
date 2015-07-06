package io.github.tcdl.api.exception;

/**
 * Created by rdro on 4/24/2015.
 */
public class JsonSchemaValidationException extends MsbException {

    public JsonSchemaValidationException(String message) {
        super(message);
    }

    public JsonSchemaValidationException(String message, Throwable cause) {
        super(message, cause);
    }

}
