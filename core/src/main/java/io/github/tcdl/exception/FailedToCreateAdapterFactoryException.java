package io.github.tcdl.exception;

/**
 * Created by anstr on 6/22/2015.
 * Exception which will be thrown in case of some problems during creation Adapter Factory object were occurred
 */
public class FailedToCreateAdapterFactoryException extends MsbException{
    public FailedToCreateAdapterFactoryException(String message) {
        super(message);
    }

    public FailedToCreateAdapterFactoryException(String message, Throwable cause) {
        super(message, cause);
    }
}
