package io.github.tcdl.msb.impl;


import io.github.tcdl.msb.api.MessageContext;

/**
 * Gives access to initial message without polluting client classes APIs MsbThreadContext is wrapper around {@link ThreadLocal}. Additional care has to be taken
 * if any kind of multithreaded message processing takes place.
 */
public class MsbThreadContext {

    private static final ThreadLocal<MessageContext> messageContext = new ThreadLocal<>();

    public static MessageContext getMessageContext() {
        return messageContext.get();
    }

    public static void setMessageContext(MessageContext messageContext) {
        MsbThreadContext.messageContext.set(messageContext);
    }

    static void clear() {
        messageContext.remove();
    }
}
