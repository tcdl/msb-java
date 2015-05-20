package io.github.tcdl;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.events.TwoArgsEventHandler;
import io.github.tcdl.exception.ChannelException;
import io.github.tcdl.messages.Message;
import io.github.tcdl.support.TestUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Created by rdro on 4/28/2015.
 */
@RunWith(MockitoJUnitRunner.class)
public class ProducerTest {

    private static final String TOPIC = "test:producer";

    @Mock
    private Adapter adapterMock;

    @Mock
    private TwoArgsEventHandler<Message, Exception> handlerMock;

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerProducerNullAdapter() {
        new Producer(null, "testTopic", handlerMock);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateProducerNullTopic() {
        new Producer(adapterMock, null, handlerMock);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullHandler() {
        new Producer(adapterMock, "testTopic", null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPublishVerifyHandlerAndCallbackCalled() {
        Message originaMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);
        TwoArgsEventHandler<Message, Exception> callbackMock = mock(TwoArgsEventHandler.class);

        Producer producer = new Producer(adapterMock, TOPIC, handlerMock);
        producer.publish(originaMessage, callbackMock);

        verify(handlerMock).onEvent(any(Message.class), eq(null));
        verify(callbackMock).onEvent(any(Message.class), eq(null));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPublishAdapterThrowExceptionVerifyCallbackIsCalled() throws ChannelException {
        Message originaMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);
        TwoArgsEventHandler<Message, Exception> callbackMock = mock(TwoArgsEventHandler.class);

        Mockito.doThrow(ChannelException.class).when(adapterMock).publish(any());

        Producer producer = new Producer(adapterMock, TOPIC, handlerMock);
        producer.publish(originaMessage, callbackMock);

        verify(handlerMock, Mockito.never()).onEvent(any(Message.class), eq(null));
        verify(callbackMock).onEvent(any(Message.class), isA(ChannelException.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPublishThrowExceptionVerifyCallbackNotSetNotCalled() throws ChannelException {
        Message originaMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);
        TwoArgsEventHandler<Message, Exception> callbackMock = mock(TwoArgsEventHandler.class);

        Mockito.doThrow(ChannelException.class).when(adapterMock).publish(any());

        Producer producer = new Producer(adapterMock, TOPIC, handlerMock);
        producer.publish(originaMessage, callbackMock);

        verify(handlerMock, Mockito.never()).onEvent(any(Message.class), eq(null));
    }

}
