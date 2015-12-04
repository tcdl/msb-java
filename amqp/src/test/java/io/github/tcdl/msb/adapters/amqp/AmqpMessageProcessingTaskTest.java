package io.github.tcdl.msb.adapters.amqp;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import io.github.tcdl.msb.adapters.ConsumerAdapter;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.rabbitmq.client.Channel;

public class AmqpMessageProcessingTaskTest {

    private String messageStr = "some message";

    private Channel mockChannel;
    private ConsumerAdapter.RawMessageHandler mockMessageHandler;
    private AmqpAcknowledgementHandler mockAcknowledgementHandler;
    
    private AmqpMessageProcessingTask task;

    @Before
    public void setUp() {
        mockChannel = mock(Channel.class);
        mockMessageHandler = mock(ConsumerAdapter.RawMessageHandler.class);
        mockAcknowledgementHandler = mock(AmqpAcknowledgementHandler.class);
        task = new AmqpMessageProcessingTask("consumer tag", messageStr, mockMessageHandler, mockAcknowledgementHandler);
    }

    @Test
    public void testMessageProcessing() throws IOException {
        task.run();

        verify(mockMessageHandler).onMessage(messageStr, mockAcknowledgementHandler);
    }

    @Test
    public void testExceptionDuringProcessing() {
        doThrow(new RuntimeException()).when(mockMessageHandler).onMessage(anyString(), any());

        try {
            task.run();
            // Verify that AMQP ack has not been sent
            verifyNoMoreInteractions(mockChannel);
        } catch (Exception e) {
            fail();
        }
    }
}