package io.github.tcdl.msb.api.exception;

/**
 * Created by anstr on 6/22/2015.
 * Exception which will be thrown in case some problems during creation Adapter Factory object were occurred
 */
public class AdapterInitializationException extends MsbException{
    public AdapterInitializationException(String message) {
        super(message);
    }

    public AdapterInitializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
