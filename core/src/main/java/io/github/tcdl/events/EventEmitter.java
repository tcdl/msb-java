package io.github.tcdl.events;

/**
 * Created by rdro on 5/25/2015.
 */
public interface EventEmitter {

    EventEmitterImpl on(Event event, GenericEventHandler eventHandler);

    EventEmitterImpl emit(Event event, Object... args);

    EventEmitterImpl once(Event event, Object... args);

}
