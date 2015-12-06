package io.github.tcdl.msb.events;

import io.github.tcdl.msb.api.AcknowledgementHandler;
import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;

import java.util.function.BiConsumer;

/**
 * {@link EventHandlers} is a component that allows to register custom event handlers for {@link Requester} specific events.
 */
public class EventHandlers<T> {

    private BiConsumer<Acknowledge, AcknowledgementHandler> onAcknowledge = (acknowledge, ackHandler) -> {};
    private BiConsumer<T, AcknowledgementHandler> onResponse = (acknowledge, ackHandler) -> {};
    private BiConsumer<Message, AcknowledgementHandler> onRawResponse = (acknowledge, ackHandler) -> {};
    private Callback<Void> onEnd = messages -> {};
    
    /**
     * Return callback registered for Acknowledge event.
     *
     * @return acknowledge callback
     */
    public BiConsumer<Acknowledge, AcknowledgementHandler> onAcknowledge() {
        return onAcknowledge;
    }

    /**
     * Registered callback for Acknowledge event.
     *
     * @param onAcknowledge callback
     * @return EventHandlers
     */
    public EventHandlers onAcknowledge(BiConsumer<Acknowledge, AcknowledgementHandler> onAcknowledge) {
        this.onAcknowledge = onAcknowledge;
        return this;
    }

    /**
     * Return callback registered for Response event.
     *
     * @return response callback
     * @return EventHandlers
     */
    public BiConsumer<T, AcknowledgementHandler> onResponse() {
        return onResponse;
    }

    /**
     * Registered callback for Response event.
     *
     * @param onResponse callback
     * @return EventHandlers
     */
    public EventHandlers onResponse(BiConsumer<T, AcknowledgementHandler> onResponse) {
        this.onResponse = onResponse;
        return this;
    }

    /**
     * Return callback registered for Response event.
     *
     * @return response callback
     * @return EventHandlers
     */
    public BiConsumer<Message, AcknowledgementHandler> onRawResponse() {
        return onRawResponse;
    }

    /**
     * Registered callback for Response event.
     *
     * @param onRawResponse callback
     * @return EventHandlers
     */
    public EventHandlers onRawResponse(BiConsumer<Message, AcknowledgementHandler> onRawResponse) {
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
}
