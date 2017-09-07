package io.github.tcdl.msb.api;

import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.impl.MsbThreadContext;

import java.util.HashMap;
import java.util.Map;

public class RetryErrorHandler implements ResponderServer.ErrorHandler {
    private Map<Class, Integer> retryExceptions = new HashMap<>();

    public <T extends Exception> RetryErrorHandler retry(Class<T> exceptionClass, int times) {
        retryExceptions.put(exceptionClass, times);
        return this;
    }

    @Override
    public void handle(Exception exception, Message originalMessage) {
        Integer times = retryExceptions.get(exception.getClass());
        if (times == null) {
            MsbThreadContext.getMessageContext().getAcknowledgementHandler().rejectMessage();
        } else {
            int retried = (Integer) MsbThreadContext.getMap().compute("retry." + exception.getClass().getName(),
                    (e, count) -> count == null || !(count instanceof Integer) ? 1 : (Integer) count + 1);
            if (retried > times) {
                MsbThreadContext.getMessageContext().getAcknowledgementHandler().retryMessage();
            } else {
                MsbThreadContext.getMessageContext().getAcknowledgementHandler().rejectMessage();
            }
        }
    }
}
