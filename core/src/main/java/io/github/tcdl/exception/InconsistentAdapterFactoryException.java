package io.github.tcdl.exception;

/**
 * Created by anstr on 6/22/2015.
 * Exception which will be thrown in case of inconsistent Adapter Factory class
 */
public class InconsistentAdapterFactoryException extends MsbException{
    public InconsistentAdapterFactoryException(String message) {
        super(message);
    }

    public InconsistentAdapterFactoryException(String message, Throwable cause) {
        super(message, cause);
    }
}
