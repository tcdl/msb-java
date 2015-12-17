package io.github.tcdl.msb.adapters.amqp;

import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AmqpMessageHandlerInvokeStrategyTest {
    @Mock
    ExecutorService mockExecutor;

    @Mock
    AcknowledgementHandlerInternal acknowledgeHandler;

    @Mock
    MessageHandler messageHandler;

    Message message = TestUtils.createMsbRequestMessage("any","any");

    @InjectMocks
    AmqpMessageHandlerInvokeStrategy adapter;

    @Test
    public void testMessageHandling() {
        adapter.execute(messageHandler, message, acknowledgeHandler);
        verify(messageHandler, never()).handleMessage(any(), any());
        ArgumentCaptor<AmqpMessageProcessingTask> taskCaptor = ArgumentCaptor.forClass(AmqpMessageProcessingTask.class);
        verify(mockExecutor, times(1)).submit(taskCaptor.capture());
        AmqpMessageProcessingTask task = taskCaptor.getValue();

        assertEquals(message, task.message);
        assertEquals(messageHandler, task.messageHandler);
        assertEquals(acknowledgeHandler, task.ackHandler);
    }
}
