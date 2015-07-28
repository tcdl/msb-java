package io.github.tcdl.msb.api.exception;

public class JsonSchemaValidationException extends MsbException {

    public JsonSchemaValidationException(String message) {
        super(message);
    }

    public JsonSchemaValidationException(String message, Throwable cause) {
        super(message, cause);
    }

}
