package io.github.tcdl.events;

/**
 * Created by rdro on 5/25/2015.
 */
public interface EventEmitter {

    EventEmitter on(Event event, GenericEventHandler eventHandler);

    EventEmitter emit(Event event, Object... args);

    EventEmitter once(Event event, Object... args);

    <A1> EventEmitter on(Event event, SingleArgEventHandler<A1> eventHandler);

    <A1, A2> EventEmitter on(Event event, TwoArgsEventHandler<A1, A2> eventHandler);

    <A1, A2, A3> EventEmitter on(Event event, ThreeArgsEventHandler<A1, A2, A3> eventHandler);

}
