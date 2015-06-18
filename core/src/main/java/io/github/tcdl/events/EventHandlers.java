package io.github.tcdl.events;

import io.github.tcdl.Callback;
import io.github.tcdl.messages.Acknowledge;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;

import java.util.List;

/**
 * {@link EventHandlers} is a component that allows to register custom event handlers for {@link io.github.tcdl.Requester} specific events .
 * <p/>
 * Created by rdrozdov-tc on 6/2/15.
 */
public class EventHandlers {

    private Callback<Acknowledge> onAcknowledge = acknowledge -> {};
    private Callback<Payload> onResponse = response -> {};
    private Callback<List<Message>> onEnd = messages -> {};
    private Callback<Exception> onError = error -> {};

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
     * @param acknowledge callback
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
    public Callback<Payload> onResponse() {
        return onResponse;
    }

    /**
     * Registered callback for Response event.
     *
     * @param response callback
     */
    public EventHandlers onResponse(Callback<Payload> onResponse) {
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
     * @param end callback
     */
    public EventHandlers onEnd(Callback<List<Message>> onEnd) {
        this.onEnd = onEnd;
        return this;
    }

    /**
     * Return callback registered for Error event.
     *
     * @return error callback
     */
    public Callback<Exception> onError() {
        return onError;
    }

    /**
     * Registered callback for Error event.
     *
     * @param error callback
     */
    public EventHandlers onError(Callback<Exception> onError) {
        this.onError = onError;
        return this;
    }
}
