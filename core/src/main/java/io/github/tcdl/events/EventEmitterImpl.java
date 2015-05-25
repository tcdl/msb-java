package io.github.tcdl.events;

import java.util.*;

/**
 * Created by rdro on 4/23/2015.
 */
public class EventEmitterImpl implements EventEmitter, ExtendedEventEmitter {

    private Map<Event, List<GenericEventHandler>> handlersByEvent =
            Collections.synchronizedMap(new HashMap<>());

    public EventEmitterImpl() {
    }

    public EventEmitterImpl on(Event event, GenericEventHandler eventHandler) {
        List<GenericEventHandler> eventHandlers = handlersByEvent.get(event);

        if (eventHandlers == null) {
            eventHandlers = Collections.synchronizedList(new ArrayList<>());
            handlersByEvent.put(event, eventHandlers);
        }

        eventHandlers.add(eventHandler);

        return this;
    }

    public <A1> EventEmitterImpl on(Event event, SingleArgEventHandler<A1> eventHandler) {
        return on(event, (GenericEventHandler)eventHandler);
    }

    public <A1, A2> EventEmitterImpl on(Event event, TwoArgsEventHandler<A1, A2> eventHandler) {
        return on(event, (GenericEventHandler)eventHandler);
    }

    public <A1, A2, A3> EventEmitterImpl on(Event event, ThreeArgsEventHandler<A1, A2, A3> eventHandler) {
        return on(event, (GenericEventHandler)eventHandler);
    }

    public EventEmitterImpl emit(Event event, Object... args) {
        List<GenericEventHandler> eventHandlers = handlersByEvent.get(event);
        if (eventHandlers == null)
            return this;

        for (GenericEventHandler eventHandler : eventHandlers) {
            eventHandler.onEvent(args);
        }

        return this;
    }

    public EventEmitterImpl once(Event event, Object... args) {
        emit(event, args).handlersByEvent.remove(event);
        return this;
    }

    public void removeListener(Event event, GenericEventHandler genericEventHandler) {
        List<GenericEventHandler> eventHandlers = handlersByEvent.get(event);
        if (eventHandlers != null) {
            eventHandlers.remove(genericEventHandler);
        }
    }

    public void removeListeners(Event event) {
        handlersByEvent.remove(event);
    }

    public void removeAllListeners() {
        handlersByEvent.clear();
    }
}
