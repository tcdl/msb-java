package io.github.tcdl.adapters.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import static io.github.tcdl.adapters.ConsumerAdapter.RawMessageHandler;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AmqpMessageConsumerTest {

    private Channel mockChannel;
    private ExecutorService mockExecutorService;
    private RawMessageHandler mockMessageHandler;

    private AmqpMessageConsumer amqpMessageConsumer;

    @Before
    public void setUp() {
        mockChannel = mock(Channel.class);
        mockExecutorService = mock(ExecutorService.class);
        mockMessageHandler = mock(RawMessageHandler.class);

        amqpMessageConsumer = new AmqpMessageConsumer(mockChannel, mockExecutorService, mockMessageHandler, Charset.forName("UTF-8"));
    }

    @Test
    public void testMessageProcessing() throws IOException {
        long deliveryTag = 1234L;
        String messageStr = "some message";
        String consumerTag = "consumer tag";
        Envelope envelope = mock(Envelope.class);
        when(envelope.getDeliveryTag()).thenReturn(deliveryTag);

        // method under test
        amqpMessageConsumer.handleDelivery(consumerTag, envelope, null, messageStr.getBytes());

        // verify that a new task has been submitted
        ArgumentCaptor<AmqpMessageProcessingTask> taskCaptor = ArgumentCaptor.forClass(AmqpMessageProcessingTask.class);
        verify(mockExecutorService).submit(taskCaptor.capture());

        // verify that the right task was submitted
        AmqpMessageProcessingTask task = taskCaptor.getValue();
        assertEquals(consumerTag, task.consumerTag);
        assertEquals(messageStr, task.body);
        assertEquals(deliveryTag, task.deliveryTag);
        assertEquals(mockMessageHandler, task.msgHandler);
        assertEquals(mockChannel, task.channel);
    }

    @Test
    public void testMessageCannotBeSubmittedForProcessing() throws IOException {
        doThrow(new RejectedExecutionException()).when(mockExecutorService).submit(any(Runnable.class));

        try {
            amqpMessageConsumer.handleDelivery("consumer tag", mock(Envelope.class), null, "some message".getBytes());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testProperCharsetUsed() throws IOException {
        byte[] encodedMessage = new byte[] { 0, 0, 0, -10 }; // In UTF-32 ö is mapped to 000000f6
        String expectedDecodedMessage = "ö";

        Envelope envelope = mock(Envelope.class);
        when(envelope.getDeliveryTag()).thenReturn(1234L);

        AmqpMessageConsumer consumer = new AmqpMessageConsumer(mockChannel, mockExecutorService, mockMessageHandler, Charset.forName("UTF-32"));
        consumer.handleDelivery("some tag", envelope, null, encodedMessage);

        ArgumentCaptor<AmqpMessageProcessingTask> taskCaptor = ArgumentCaptor.forClass(AmqpMessageProcessingTask.class);
        verify(mockExecutorService).submit(taskCaptor.capture());
        assertEquals(expectedDecodedMessage, taskCaptor.getValue().body);
    }

}