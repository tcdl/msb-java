package io.github.tcdl.msb.adapters.mock;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.adapters.ProducerAdapter;
import io.github.tcdl.msb.api.exception.ChannelException;
import io.github.tcdl.msb.api.exception.JsonConversionException;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.support.TestUtils;
import io.github.tcdl.msb.support.Utils;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * MockAdapter class represents implementation of {@link ProducerAdapter} and {@link ConsumerAdapter}
 * for test purposes.
 * 
 */
public class MockAdapterTest {

    private ObjectMapper messageMapper = TestUtils.createMessageMapper();

    @Test
    public void testPublishAddMessageToMap() throws ChannelException, JsonConversionException {
        String topic = "test:mock-adapter-publish";
        Message message = TestUtils.createSimpleRequestMessage(topic);
        MockAdapter mockAdapter = new MockAdapter(topic);

        mockAdapter.publish(Utils.toJson(message, messageMapper));

        assertNotNull(mockAdapter.messageMap.get(topic).poll());
    }

    @Test
    public void testSubscribeCallMessageHandler() throws ChannelException, JsonConversionException {
        String topic = "test:mock-adapter-subscribe";
        String message = Utils.toJson(TestUtils.createSimpleRequestMessage(topic), messageMapper);
        Queue<ExecutorService> activeConsumerExecutors = new LinkedList<>();
        MockAdapter mockAdapter = new MockAdapter(topic, activeConsumerExecutors);
        Queue<String> messages = new ConcurrentLinkedQueue<>();
        messages.add(message);
        mockAdapter.messageMap.put(topic, messages);
        ConsumerAdapter.RawMessageHandler mockHandler = mock(ConsumerAdapter.RawMessageHandler.class);

        mockAdapter.subscribe(mockHandler);

        assertTrue(activeConsumerExecutors.size() == 1);
        verify(mockHandler, timeout(500)).onMessage(eq(message), any());
    }

    @Test
    public void testUnsubscribe() throws ChannelException, JsonConversionException {
        String topic = "test:mock-adapter-unsubscribe";
        Queue<ExecutorService> activeConsumerExecutors = new LinkedList<>();
        MockAdapter mockAdapter = new MockAdapter(topic, activeConsumerExecutors);

        ConsumerAdapter.RawMessageHandler mockHandler = mock(ConsumerAdapter.RawMessageHandler.class);
        mockAdapter.subscribe(mockHandler);

        assertTrue(activeConsumerExecutors.size() == 1);
        mockAdapter.unsubscribe();
        assertTrue(activeConsumerExecutors.size() == 0);

    }

}
