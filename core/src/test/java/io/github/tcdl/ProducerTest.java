package io.github.tcdl;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.events.TwoArgsEventHandler;
import io.github.tcdl.messages.Message;
import io.github.tcdl.support.TestUtils;

import java.util.Random;

import org.junit.Test;

/**
 * Created by rdro on 4/28/2015.
 */
public class ProducerTest {
    
    @Test(expected = NullPointerException.class)
    public void testCreateConsumerProducerNullAdapter() {
        TwoArgsEventHandler<Message, Exception> handler = mock(TwoArgsEventHandler.class);
        MsbConfigurations msbConf = mock(MsbConfigurations.class);
        new Producer(null, "testTopic", handler);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateProducerNullTopic() {
        Adapter adapter = mock(Adapter.class);
        TwoArgsEventHandler<Message, Exception> handler = mock(TwoArgsEventHandler.class);
        MsbConfigurations msbConf = mock(MsbConfigurations.class);
        new Producer(adapter, null, handler);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateConsumerNullHandler() {
        Adapter adapter = mock(Adapter.class);
        MsbConfigurations msbConf = mock(MsbConfigurations.class);
        new Producer(adapter, "testTopic", null);
    }

    @Test
    public void testPublishToTopicVerifyHandlerAndConsumerCalled() {        
        String producerTopic = "producerTopic:" + new Random().nextInt(1000);
        Adapter adapterMock = mock(Adapter.class);
        TwoArgsEventHandler<Message, Exception> handlerMock = mock(TwoArgsEventHandler.class);
        TwoArgsEventHandler<Message, Exception> callbackMock = mock(TwoArgsEventHandler.class);

        Message originaMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(producerTopic);
        Producer producer = new Producer(adapterMock, producerTopic, handlerMock);
        producer.publish(originaMessage, callbackMock);        

        verify(handlerMock).onEvent(any(Message.class), eq(null));
        verify(callbackMock).onEvent(any(Message.class), eq(null));
    }
}
