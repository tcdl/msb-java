package io.github.tcdl.msb.adapters.mock;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.adapters.ProducerAdapter;
import io.github.tcdl.msb.api.exception.ChannelException;
import io.github.tcdl.msb.api.exception.JsonConversionException;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.support.TestUtils;
import io.github.tcdl.msb.support.Utils;
import org.junit.Test;

/**
 * MockAdapter class represents implementation of {@link ProducerAdapter} and {@link ConsumerAdapter}
 * for test purposes.
 * 
 */
public class MockAdapterTest {

    @Test
    public void testPublishAddMessageToMap() throws ChannelException, JsonConversionException {
        String topic = "test:mock-adapter-publish";
        Message message = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(topic);
        MockAdapter mockAdapter = new MockAdapter(topic);

        mockAdapter.publish(Utils.toJson(message));

        assertNotNull(mockAdapter.messageMap.get(topic).poll());
    }

    @Test
    public void testSubscribeCallMessageHandler() throws ChannelException, JsonConversionException {
        String topic = "test:mock-adapter-subscribe";
        String message = Utils.toJson(TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(topic));
        Queue<String> queue = new ConcurrentLinkedQueue<>();
        queue.add(message);
        MockAdapter mockAdapter = new MockAdapter(topic);
        mockAdapter.messageMap.put(topic, queue);
        ConsumerAdapter.RawMessageHandler mockHandler = mock(ConsumerAdapter.RawMessageHandler.class);

        mockAdapter.subscribe(mockHandler);

        verify(mockHandler, timeout(500)).onMessage(eq(message));
    }

}
