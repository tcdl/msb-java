package io.github.tcdl;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import io.github.tcdl.adapters.Adapter;
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
    private Callback<Message> handlerMock;

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

        Producer producer = new Producer(adapterMock, TOPIC, handlerMock);
        producer.publish(originaMessage);

        verify(handlerMock).call(any(Message.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPublishAdapterThrowExceptionVerifyCallbackIsCalled() throws ChannelException {
        Message originaMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);

        Mockito.doThrow(ChannelException.class).when(adapterMock).publish(any());

        Producer producer = new Producer(adapterMock, TOPIC, handlerMock);
        producer.publish(originaMessage);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPublishThrowExceptionVerifyCallbackNotSetNotCalled() throws ChannelException {
        Message originaMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);

        Mockito.doThrow(ChannelException.class).when(adapterMock).publish(any());

        Producer producer = new Producer(adapterMock, TOPIC, handlerMock);
        producer.publish(originaMessage);
    }

}
