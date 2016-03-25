package io.github.tcdl.msb.events;

import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.MessageContext;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;

import java.util.function.BiConsumer;

/**
 * {@link EventHandlers} is a component that allows to register custom event handlers for {@link Requester} specific events.
 */
public class EventHandlers<T> {

    private BiConsumer<Acknowledge, MessageContext> onAcknowledge = (acknowledge, msgContext) -> {};
    private BiConsumer<T, MessageContext> onResponse = (acknowledge, msgContext) -> {};
    private BiConsumer<Message, MessageContext> onRawResponse = (acknowledge, msgContext) -> {};
    private Callback<Void> onEnd = messages -> {};
    private BiConsumer<Exception, Message> onError;

    /**
     * Return callback registered for Acknowledge event.
     *
     * @return acknowledge callback
     */
    public BiConsumer<Acknowledge, MessageContext> onAcknowledge() {
        return onAcknowledge;
    }

    /**
     * Registered callback for Acknowledge event.
     *
     * @param onAcknowledge callback
     * @return EventHandlers
     */
    public EventHandlers onAcknowledge(BiConsumer<Acknowledge, MessageContext> onAcknowledge) {
        this.onAcknowledge = onAcknowledge;
        return this;
    }

    /**
     * Return callback registered for Response event.
     *
     * @return response callback
     * @return EventHandlers
     */
    public BiConsumer<T, MessageContext> onResponse() {
        return onResponse;
    }

    /**
     * Registered callback for Response event.
     *
     * @param onResponse callback
     * @return EventHandlers
     */
    public EventHandlers onResponse(BiConsumer<T, MessageContext> onResponse) {
        this.onResponse = onResponse;
        return this;
    }

    /**
     * Return callback registered for Response event.
     *
     * @return response callback
     * @return EventHandlers
     */
    public BiConsumer<Message, MessageContext> onRawResponse() {
        return onRawResponse;
    }

    /**
     * Registered callback for Response event.
     *
     * @param onRawResponse callback
     * @return EventHandlers
     */
    public EventHandlers onRawResponse(BiConsumer<Message, MessageContext> onRawResponse) {
        this.onRawResponse = onRawResponse;
        return this;
    }

    /**
     * Return callback registered for End event.
     *
     * @return end event callback
     */
    public Callback<Void> onEnd() {
        return onEnd;
    }

    /**
     * Registered callback for End event.
     *
     * @param onEnd callback
     * @return EventHandlers
     */
    public EventHandlers onEnd(Callback<Void> onEnd) {
        this.onEnd = onEnd;
        return this;
    }

    /**
     * Registered callback for Error event.
     *
     * @param onError callback
     * @return EventHandlers
     */
    public EventHandlers onError(BiConsumer<Exception, Message>  onError) {
        this.onError = onError;
        return this;
    }

    /**
     * Return callback registered for Error event.
     *
     * @return error callback
     */
    public BiConsumer<Exception, Message> onError() {
        return onError;
    }

}
