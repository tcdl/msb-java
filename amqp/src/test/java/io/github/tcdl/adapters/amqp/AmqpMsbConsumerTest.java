package io.github.tcdl.adapters.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import static io.github.tcdl.adapters.ConsumerAdapter.RawMessageHandler;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AmqpMsbConsumerTest {

    private Channel mockChannel;
    private ExecutorService mockExecutorService;
    private RawMessageHandler mockMessageHandler;

    private AmqpMsbConsumer amqpMsbConsumer;

    @Before
    public void setUp() {
        mockChannel = mock(Channel.class);
        mockExecutorService = mock(ExecutorService.class);
        mockMessageHandler = mock(RawMessageHandler.class);

        amqpMsbConsumer = new AmqpMsbConsumer(mockChannel, mockExecutorService, mockMessageHandler);
    }

    @Test
    public void testMessageProcessing() throws IOException {
        long deliveryTag = 1234L;
        String messageStr = "some message";
        Envelope envelope = mock(Envelope.class);
        when(envelope.getDeliveryTag()).thenReturn(deliveryTag);

        // method under test
        amqpMsbConsumer.handleDelivery("consumer tag", envelope, null, messageStr.getBytes());

        // verify that a new task has been submitted
        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(mockExecutorService).submit(taskCaptor.capture());

        // verify that the task does right things
        Runnable task = taskCaptor.getValue();
        task.run();

        verify(mockMessageHandler).onMessage(messageStr);
        verify(mockChannel).basicAck(deliveryTag, false);
    }

    @Test
    public void testMessageCannotBeSubmittedForProcessing() throws IOException {
        doThrow(new RejectedExecutionException()).when(mockExecutorService).submit(any(Runnable.class));

        try {
            amqpMsbConsumer.handleDelivery("consumer tag", mock(Envelope.class), null, "some message".getBytes());
        } catch (Exception e) {
            Assert.fail();
        }
    }
}