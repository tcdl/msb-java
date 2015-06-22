package io.github.tcdl.exception;

/**
 * Created by anstr on 6/22/2015.
 * Exception which will be thrown in case not supported Adapter Factory class
 */
public class AdapterFactoryNotSupportedException extends MsbException{
    public AdapterFactoryNotSupportedException(String message) {
        super(message);
    }

    public AdapterFactoryNotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }
}
