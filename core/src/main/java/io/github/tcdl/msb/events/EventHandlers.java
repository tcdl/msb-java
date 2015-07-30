package io.github.tcdl.msb.events;

import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.Payload;

import java.util.List;

/**
 * {@link EventHandlers} is a component that allows to register custom event handlers for {@link Requester} specific events .
 * Created by rdrozdov-tc on 6/2/15.
 */
public class EventHandlers<T extends Payload> {

    private Callback<Acknowledge> onAcknowledge = acknowledge -> {};
    private Callback<T> onResponse = response -> {};
    private Callback<List<Message>> onEnd = messages -> {};

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
     * Return callback registered for End event.
     *
     * @return end event callback
     */
    public Callback<List<Message>> onEnd() {
        return onEnd;
    }

    /**
     * Registered callback for End event.
     *
     * @param onEnd callback
     */
    public EventHandlers onEnd(Callback<List<Message>> onEnd) {
        this.onEnd = onEnd;
        return this;
    }
}
