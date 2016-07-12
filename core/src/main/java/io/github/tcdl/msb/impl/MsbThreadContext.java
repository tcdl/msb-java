package io.github.tcdl.msb.impl;


import io.github.tcdl.msb.api.MessageContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Gives access to initial message without polluting client classes APIs MsbThreadContext is wrapper around {@link ThreadLocal}. Additional care has to be taken
 * if any kind of multithreaded message processing takes place.
 */
public class MsbThreadContext {

    private static final ThreadLocal<MessageContext> messageContext = new ThreadLocal<>();
    private static final ThreadLocal<Object> request = new ThreadLocal<>();
    private static final ThreadLocal<Map<String, Object>> map = new ThreadLocal<Map<String, Object>>(){
        @Override
        protected Map<String, Object> initialValue() {
            return new HashMap<>();
        }
    };

    public static MessageContext getMessageContext() {
        return messageContext.get();
    }

    public static void setMessageContext(MessageContext messageContext) {
        MsbThreadContext.messageContext.set(messageContext);
    }

    public static Object getRequest() {
        return request.get();
    }

    public static void setRequest(Object request) {
        MsbThreadContext.request.set(request);
    }


    public static Map<String, Object> getMap() {
        return map.get();
    }

    public static void put(String key, Object value){
        if(key == null) {
            throw new IllegalArgumentException("key cannot be null");
        }

        map.get().put(key, value);
    }


    static void clear() {
        messageContext.remove();
        request.remove();
        map.remove();
    }

}
