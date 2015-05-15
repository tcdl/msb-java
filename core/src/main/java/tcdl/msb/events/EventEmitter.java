package tcdl.msb.events;

import java.util.*;

/**
 * Created by rdro on 4/23/2015.
 */
public class EventEmitter {

    private Map<Event, List<EventHandler>> handlersByEvent =
            Collections.synchronizedMap(new HashMap<Event, List<EventHandler>>());

    public EventEmitter() {
    }

    public EventEmitter on(Event event, EventHandler eventHandler) {
        List<EventHandler> eventHandlers = handlersByEvent.get(event);

        if (eventHandlers == null) {
            eventHandlers = Collections.synchronizedList(new ArrayList<EventHandler>());
            handlersByEvent.put(event, eventHandlers);
        }

        eventHandlers.add(eventHandler);

        return this;
    }

    public EventEmitter emit(Event event, Object... args) {
        List<EventHandler> eventHandlers = handlersByEvent.get(event);
        if (eventHandlers == null) return this;

        for (EventHandler eventHandler : eventHandlers) {
            eventHandler.onEvent(args);
        }

        return this;
    }

    public EventEmitter once(Event event, Object... args) {
        emit(event, args).handlersByEvent.remove(event);
        return this;
    }

    public void removeListener(Event event, EventHandler eventHandler) {
        List<EventHandler> eventHandlers = handlersByEvent.get(event);
        if (eventHandlers != null) {
            eventHandlers.remove(eventHandler);
        }
    }

    public void removeListeners(Event event) {
        handlersByEvent.remove(event);
    }

    public void removeAllListeners() {
        handlersByEvent.clear();
    }
}
