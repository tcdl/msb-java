package io.github.tcdl.msb.threading;

import com.typesafe.config.ConfigFactory;
import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.support.TestUtils;
import io.github.tcdl.msb.threading.ThreadPoolMessageHandlerInvoker;
import io.github.tcdl.msb.threading.MessageProcessingTask;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ThreadPoolMessageHandlerInvokerImplTest {

    private static final int CONFIG_THREADS = 5;
    private static final int CONFIG_QUEUE = -1;

    @Mock
    ExecutorService mockExecutor;

    @Mock
    AcknowledgementHandlerInternal acknowledgeHandler;

    @Mock
    MessageHandler messageHandler;

    @Mock
    ConsumerExecutorFactory consumerExecutorFactory;

    Message message = TestUtils.createMsbRequestMessage("any","any");

    ThreadPoolMessageHandlerInvoker invoker;

    @Before
    public void setUp() throws Exception {

        when(consumerExecutorFactory.createConsumerThreadPool(CONFIG_THREADS, CONFIG_QUEUE)).thenReturn(mockExecutor);

        invoker = new ThreadPoolMessageHandlerInvoker(CONFIG_THREADS, CONFIG_QUEUE, consumerExecutorFactory);

        when(mockExecutor.awaitTermination(10, TimeUnit.SECONDS)).thenReturn(true);
    }

    @Test
    public void testSingleExecutorInitialized() {
        verify(consumerExecutorFactory, times(1)).createConsumerThreadPool(CONFIG_THREADS, CONFIG_QUEUE);
    }

    @Test
    public void testMessageHandling() {
        invoker.execute(messageHandler, message, acknowledgeHandler);
        verify(messageHandler, never()).handleMessage(any(), any());
        ArgumentCaptor<MessageProcessingTask> taskCaptor = ArgumentCaptor.forClass(MessageProcessingTask.class);
        verify(mockExecutor, times(1)).submit(taskCaptor.capture());
        MessageProcessingTask task = taskCaptor.getValue();

        assertEquals(message, task.getMessage());
        assertEquals(messageHandler, task.getMessageHandler());
        assertEquals(acknowledgeHandler, task.getAckHandler());
    }

    @Test
    public void testShutdown() {
        verify(mockExecutor, times(0)).shutdown();
        invoker.shutdown();
        verify(mockExecutor, times(1)).shutdown();
    }
}