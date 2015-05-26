package io.github.tcdl.events;

/**
 * Created by rdro on 5/25/2015.
 */
public interface ExtendedEventEmitter {

    <A1> ExtendedEventEmitter on(Event event, SingleArgEventHandler<A1> eventHandler);

    <A1, A2> ExtendedEventEmitter on(Event event, TwoArgsEventHandler<A1, A2> eventHandler);

    <A1, A2, A3> ExtendedEventEmitter on(Event event, ThreeArgsEventHandler<A1, A2, A3> eventHandler);

}
