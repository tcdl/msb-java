package io.github.tcdl.adapters.amqp;

import com.rabbitmq.client.Channel;
import io.github.tcdl.adapters.ConsumerAdapter;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class AmqpMessageProcessingTaskTest {

    private String messageStr = "some message";
    private long deliveryTag = 123L;

    private Channel mockChannel;
    private ConsumerAdapter.RawMessageHandler mockMessageHandler;

    private AmqpMessageProcessingTask task;

    @Before
    public void setUp() {
        mockChannel = mock(Channel.class);
        mockMessageHandler = mock(ConsumerAdapter.RawMessageHandler.class);
        task = new AmqpMessageProcessingTask("consumer tag", messageStr, mockChannel, deliveryTag, mockMessageHandler);
    }

    @Test
    public void testMessageProcessing() throws IOException {
        task.run();

        verify(mockMessageHandler).onMessage(messageStr);
        verify(mockChannel).basicAck(deliveryTag, false);
    }

    @Test
    public void testExceptionDuringProcessing() {
        doThrow(new RuntimeException()).when(mockMessageHandler).onMessage(anyString());

        try {
            task.run();
            // Verify that AMQP ack has not been sent
            verifyNoMoreInteractions(mockChannel);
        } catch (Exception e) {
            fail();
        }
    }
}