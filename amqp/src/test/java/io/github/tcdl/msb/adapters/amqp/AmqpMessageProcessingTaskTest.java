package io.github.tcdl.msb.adapters.amqp;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import io.github.tcdl.msb.support.TestUtils;

import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.adapters.ConsumerAdapter;

import java.io.IOException;

import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerImpl;
import io.github.tcdl.msb.api.message.Message;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.rabbitmq.client.Channel;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AmqpMessageProcessingTaskTest {

    private String messageStr = "some message";

    private Message message;
    @Mock
    private Channel mockChannel;

    @Mock
    private MessageHandler mockMessageHandler;

    @Mock
    private AcknowledgementHandlerImpl mockAcknowledgementHandler;

    private AmqpMessageProcessingTask task;

    @Before
    public void setUp() {
        message = TestUtils.createSimpleRequestMessage("any");
        task = new AmqpMessageProcessingTask(mockMessageHandler, message, mockAcknowledgementHandler);
    }

    @Test
    public void testMessageProcessing() throws IOException {
        task.run();
        verify(mockMessageHandler).handleMessage(any(), eq(mockAcknowledgementHandler));
        verify(mockAcknowledgementHandler, times(1)).autoConfirm();
        verifyNoMoreInteractions(mockAcknowledgementHandler);
    }

    @Test
    public void testExceptionDuringProcessing() {
        doThrow(new RuntimeException()).when(mockMessageHandler).handleMessage(any(), any());

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