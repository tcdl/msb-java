package io.github.tcdl.msb.events;

import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;

import java.util.function.BiConsumer;

/**
 * {@link EventHandlers} is a component that allows to register custom event handlers for {@link Requester} specific events.
 */
public class EventHandlers<T> {

    private BiConsumer<Acknowledge, ConsumerAdapter.AcknowledgementHandler> onAcknowledge = (acknowledge, ackHandler) -> {};
    private BiConsumer<T, ConsumerAdapter.AcknowledgementHandler> onResponse = (acknowledge, ackHandler) -> {};
    private BiConsumer<Message, ConsumerAdapter.AcknowledgementHandler> onRawResponse = (acknowledge, ackHandler) -> {};
    private Callback<Void> onEnd = messages -> {};
    
    /**
     * Return callback registered for Acknowledge event.
     *
     * @return acknowledge callback
     */
    public BiConsumer<Acknowledge, ConsumerAdapter.AcknowledgementHandler> onAcknowledge() {
        return onAcknowledge;
    }

    /**
     * Registered callback for Acknowledge event.
     *
     * @param onAcknowledge callback
     */
    public EventHandlers onAcknowledge(BiConsumer<Acknowledge, ConsumerAdapter.AcknowledgementHandler> onAcknowledge) {
        this.onAcknowledge = onAcknowledge;
        return this;
    }

    /**
     * Return callback registered for Response event.
     *
     * @return response callback
     */
    public BiConsumer<T, ConsumerAdapter.AcknowledgementHandler> onResponse() {
        return onResponse;
    }

    /**
     * Registered callback for Response event.
     *
     * @param onResponse callback
     */
    public EventHandlers onResponse(BiConsumer<T, ConsumerAdapter.AcknowledgementHandler> onResponse) {
        this.onResponse = onResponse;
        return this;
    }

    /**
     * Return callback registered for Response event.
     *
     * @return response callback
     */
    public BiConsumer<Message, ConsumerAdapter.AcknowledgementHandler> onRawResponse() {
        return onRawResponse;
    }

    /**
     * Registered callback for Response event.
     *
     * @param onRawResponse callback
     */
    public EventHandlers onRawResponse(BiConsumer<Message, ConsumerAdapter.AcknowledgementHandler> onRawResponse) {
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
     */
    public EventHandlers onEnd(Callback<Void> onEnd) {
        this.onEnd = onEnd;
        return this;
    }
}
