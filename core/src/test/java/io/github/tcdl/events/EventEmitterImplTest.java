package io.github.tcdl.events;

import org.junit.Test;

import static org.mockito.Mockito.*;

/**
 * Created by rdro on 5/26/2015.
 */
public class EventEmitterImplTest {

    private EventEmitterImpl eventEmitter = new EventEmitterImpl();

    @Test
    public void testGenericEmitAndOn() {
        Object arg = new Object();
        GenericEventHandler eventHandlerMock = mock(GenericEventHandler.class);
        eventEmitter.on(Event.MESSAGE_EVENT, eventHandlerMock);
        eventEmitter.emit(Event.MESSAGE_EVENT, arg);

        verify(eventHandlerMock).onEvent(eq(arg));
    }

    @Test
    public void testSingleArgEmitAndOn() {
        Object arg = new Object();
        GenericEventHandler eventHandlerMock = mock(SingleArgEventHandler.class);
        eventEmitter.on(Event.MESSAGE_EVENT, eventHandlerMock);
        eventEmitter.emit(Event.MESSAGE_EVENT, arg);

        verify(eventHandlerMock).onEvent(eq(arg));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSingleArgEmitAndOnError() {
        eventEmitter.on(Event.MESSAGE_EVENT, arg -> {
        });
        eventEmitter.emit(Event.MESSAGE_EVENT, new Object(), new Object());
    }

    @Test
    public void testTwoArgsEmitAndOn() {
        Object arg1 = new Object(), arg2 = new Object();
        GenericEventHandler eventHandlerMock = mock(TwoArgsEventHandler.class);
        eventEmitter.on(Event.MESSAGE_EVENT, eventHandlerMock);
        eventEmitter.emit(Event.MESSAGE_EVENT, arg1, arg2);

        verify(eventHandlerMock).onEvent(eq(arg1), eq(arg2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTwoArgsEmitAndOnError() {
        eventEmitter.on(Event.MESSAGE_EVENT, (arg1, arg2) -> {
        });
        eventEmitter.emit(Event.MESSAGE_EVENT, new Object(), new Object(), new Object());
    }

    @Test
    public void testThreeArgsEmitAndOn() {
        Object arg1 = new Object(), arg2 = new Object(), arg3 = new Object();
        GenericEventHandler eventHandlerMock = mock(ThreeArgsEventHandler.class);
        eventEmitter.on(Event.MESSAGE_EVENT, eventHandlerMock);
        eventEmitter.emit(Event.MESSAGE_EVENT, arg1, arg2, arg3);

        verify(eventHandlerMock).onEvent(eq(arg1), eq(arg2), eq(arg3));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThreeArgsEmitAndOnError() {
        eventEmitter.on(Event.MESSAGE_EVENT, (arg1, arg2, arg3) -> {
        });
        eventEmitter.emit(Event.MESSAGE_EVENT, new Object(), new Object(), new Object(), new Object());
    }

    @Test
    public void removeListenersForEvent() {
        Object arg = new Object();
        GenericEventHandler eventHandlerMock = mock(GenericEventHandler.class);
        eventEmitter.on(Event.MESSAGE_EVENT, eventHandlerMock);
        eventEmitter.emit(Event.MESSAGE_EVENT, arg);

        verify(eventHandlerMock).onEvent(eq(arg));
        reset(eventHandlerMock);

        eventEmitter.removeListeners(Event.MESSAGE_EVENT);
        eventEmitter.emit(Event.MESSAGE_EVENT, arg);

        verify(eventHandlerMock, never()).onEvent(eq(arg));
    }

    @Test
    public void removeListener() {
        Object arg = new Object();
        GenericEventHandler eventHandlerMock = mock(GenericEventHandler.class);
        eventEmitter.on(Event.MESSAGE_EVENT, eventHandlerMock);
        eventEmitter.emit(Event.MESSAGE_EVENT, arg);

        verify(eventHandlerMock).onEvent(eq(arg));
        reset(eventHandlerMock);

        eventEmitter.removeListener(Event.MESSAGE_EVENT, eventHandlerMock);
        eventEmitter.emit(Event.MESSAGE_EVENT, arg);

        verify(eventHandlerMock, never()).onEvent(eq(arg));
    }

    @Test
    public void removeAllListeners() {
        Object arg = new Object();
        GenericEventHandler eventHandlerMock = mock(GenericEventHandler.class);
        eventEmitter.on(Event.MESSAGE_EVENT, eventHandlerMock);
        eventEmitter.emit(Event.MESSAGE_EVENT, arg);

        verify(eventHandlerMock).onEvent(eq(arg));
        reset(eventHandlerMock);

        eventEmitter.removeAllListeners();
        eventEmitter.emit(Event.MESSAGE_EVENT, arg);

        verify(eventHandlerMock, never()).onEvent(eq(arg));
    }


}
