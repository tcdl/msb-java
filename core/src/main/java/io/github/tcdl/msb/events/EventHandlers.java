package io.github.tcdl.msb.events;

import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.Payload;

/**
 * {@link EventHandlers} is a component that allows to register custom event handlers for {@link Requester} specific events.
 */
public class EventHandlers<T extends Payload> {

    private Callback<Acknowledge> onAcknowledge = acknowledge -> {};
    private Callback<T> onResponse = response -> {};
    private Callback<Message> onRawResponse = response -> {};
    private Callback<Void> onEnd = messages -> {};

    /**
     * Return callback registered for Acknowledge event.
     *
     * @return acknowledge callback
     */
    public Callback<Acknowledge> onAcknowledge() {
        return onAcknowledge;
    }

    /**
     * Registered callback for Acknowledge event.
     *
     * @param onAcknowledge callback
     */
    public EventHandlers onAcknowledge(Callback<Acknowledge> onAcknowledge) {
        this.onAcknowledge = onAcknowledge;
        return this;
    }

    /**
     * Return callback registered for Response event.
     *
     * @return response callback
     */
    public Callback<T> onResponse() {
        return onResponse;
    }

    /**
     * Registered callback for Response event.
     *
     * @param onResponse callback
     */
    public EventHandlers onResponse(Callback<T> onResponse) {
        this.onResponse = onResponse;
        return this;
    }

    /**
     * Return callback registered for Response event.
     *
     * @return response callback
     */
    public Callback<Message> onRawResponse() {
        return onRawResponse;
    }

    /**
     * Registered callback for Response event.
     *
     * @param onRawResponse callback
     */
    public EventHandlers onRawResponse(Callback<Message> onRawResponse) {
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
