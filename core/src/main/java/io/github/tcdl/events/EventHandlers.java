package io.github.tcdl.events;

import io.github.tcdl.Callback;
import io.github.tcdl.messages.Acknowledge;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;

import java.util.List;

/**
 * Created by rdrozdov-tc on 6/2/15.
 */
public class EventHandlers {

    private Callback<Acknowledge> onAcknowledge = acknowledge -> {};
    private Callback<Payload> onResponse = response -> {};
    private Callback<List<Message>> onEnd = messages -> {};
    private Callback<Exception> onError = error -> {};

    public Callback<Acknowledge> onAcknowledge() {
        return onAcknowledge;
    }

    public EventHandlers onAcknowledge(Callback<Acknowledge> onAcknowledge) {
        this.onAcknowledge = onAcknowledge;
        return this;
    }

    public Callback<Payload> onResponse() {
        return onResponse;
    }

    public EventHandlers onResponse(Callback<Payload> onResponse) {
        this.onResponse = onResponse;
        return this;
    }

    public Callback<List<Message>> onEnd() {
        return onEnd;
    }

    public EventHandlers onEnd(Callback<List<Message>> onEnd) {
        this.onEnd = onEnd;
        return this;
    }

    public Callback<Exception> onError() {
        return onError;
    }

    public EventHandlers onError(Callback<Exception> onError) {
        this.onError = onError;
        return this;
    }
}
