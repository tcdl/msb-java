package io.github.tcdl.events;

import java.util.*;

/**
 * Created by rdro on 4/23/2015.
 */
public class EventEmitter {

    private Map<Event, List<GenericEventHandler>> handlersByEvent =
            Collections.synchronizedMap(new HashMap<>());

    public EventEmitter() {
    }

    public EventEmitter on(Event event, GenericEventHandler eventHandler) {
        List<GenericEventHandler> eventHandlers = handlersByEvent.get(event);

        if (eventHandlers == null) {
            eventHandlers = Collections.synchronizedList(new ArrayList<>());
            handlersByEvent.put(event, eventHandlers);
        }

        eventHandlers.add(eventHandler);

        return this;
    }

    public <A1> EventEmitter on(Event event, SingleArgEventHandler<A1> eventHandler) {
        return on(event, (GenericEventHandler)eventHandler);
    }

    public <A1, A2> EventEmitter on(Event event, TwoArgsEventHandler<A1, A2> eventHandler) {
        return on(event, (GenericEventHandler)eventHandler);
    }

    public <A1, A2, A3> EventEmitter on(Event event, ThreeArgsEventHandler<A1, A2, A3> eventHandler) {
        return on(event, (GenericEventHandler)eventHandler);
    }

    public EventEmitter emit(Event event, Object... args) {
        List<GenericEventHandler> GenericEventHandlers = handlersByEvent.get(event);
        if (GenericEventHandlers == null)
            return this;

        for (GenericEventHandler GenericEventHandler : GenericEventHandlers) {
            GenericEventHandler.onEvent(args);
        }

        return this;
    }

    public EventEmitter once(Event event, Object... args) {
        emit(event, args).handlersByEvent.remove(event);
        return this;
    }

    public void removeListener(Event event, GenericEventHandler GenericEventHandler) {
        List<GenericEventHandler> GenericEventHandlers = handlersByEvent.get(event);
        if (GenericEventHandlers != null) {
            GenericEventHandlers.remove(GenericEventHandler);
        }
    }

    public void removeListeners(Event event) {
        handlersByEvent.remove(event);
    }

    public void removeAllListeners() {
        handlersByEvent.clear();
    }
}
