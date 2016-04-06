package io.github.tcdl.msb.api;


import io.github.tcdl.msb.api.message.Message;

/**
 * Gives access to initial message without polluting client classes APIs
 * FlowContext is wrapper around {@link ThreadLocal}. Additional care has to be taken if any kind of multithreaded message processing takes place.
 */
public class FlowContext {

    private static final ThreadLocal<Message> initialMessage = new ThreadLocal<>();

    public static void setInitialMessage(Message message){
        initialMessage.set(message);
    }

    public static Message getInitialMessage(){
        return initialMessage.get();
    }

    public static void clear(){
        initialMessage.remove();
    }
}
