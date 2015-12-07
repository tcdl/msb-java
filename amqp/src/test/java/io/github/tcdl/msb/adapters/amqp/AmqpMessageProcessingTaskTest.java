package io.github.tcdl.msb.adapters.amqp;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

import io.github.tcdl.msb.adapters.ConsumerAdapter;

import java.io.IOException;

import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerImpl;
import org.junit.Before;
import org.junit.Test;

import com.rabbitmq.client.Channel;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AmqpMessageProcessingTaskTest {

    private String messageStr = "some message";

    @Mock
    private Channel mockChannel;

    @Mock
    private ConsumerAdapter.RawMessageHandler mockMessageHandler;

    @Mock
    private AcknowledgementHandlerImpl mockAcknowledgementHandler;

    private AmqpMessageProcessingTask task;

    @Before
    public void setUp() {
        task = new AmqpMessageProcessingTask("consumer tag", messageStr, mockMessageHandler, mockAcknowledgementHandler);
    }

    @Test
    public void testMessageProcessing() throws IOException {
        task.run();
        verify(mockMessageHandler).onMessage(messageStr, mockAcknowledgementHandler);
        verify(mockAcknowledgementHandler, times(1)).autoConfirm();
        verifyNoMoreInteractions(mockAcknowledgementHandler);
    }

    @Test
    public void testExceptionDuringProcessing() {
        doThrow(new RuntimeException()).when(mockMessageHandler).onMessage(anyString(), any());

        try {
            task.run();
            // Verify that AMQP ack has not been sent
            verifyNoMoreInteractions(mockChannel);

            verify(mockAcknowledgementHandler, times(1)).autoRetry();
            verifyNoMoreInteractions(mockAcknowledgementHandler);
        } catch (Exception e) {
            fail();
        }
    }
}