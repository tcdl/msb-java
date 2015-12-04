package io.github.tcdl.msb.adapters.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.config.amqp.AmqpBrokerConfig;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;

public class AmqpMessageConsumerTest {

    private static final boolean REQUEUE_REJECTED_MESSAGES = true;

    private Channel mockChannel;
    private ExecutorService mockExecutorService;
    private ConsumerAdapter.RawMessageHandler mockMessageHandler;
    private AmqpBrokerConfig mockBrokerConfig;

    private AmqpMessageConsumer amqpMessageConsumer;

    @Before
    public void setUp() {
        mockChannel = mock(Channel.class);
        mockExecutorService = mock(ExecutorService.class);
        mockMessageHandler = mock(ConsumerAdapter.RawMessageHandler.class);
        mockBrokerConfig = mock(AmqpBrokerConfig.class);

        when(mockBrokerConfig.getCharset()).thenReturn(Charset.forName("UTF-8"));
        when(mockBrokerConfig.isRequeueRejectedMessages()).thenReturn(REQUEUE_REJECTED_MESSAGES);

        amqpMessageConsumer = new AmqpMessageConsumer(mockChannel, mockExecutorService, mockMessageHandler, mockBrokerConfig);
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
        assertEquals(mockMessageHandler, task.msgHandler);        
    }

    @Test
    public void testMessageCannotBeSubmittedForProcessing() throws IOException {
        long deliveryTag = 1234L;
        Envelope envelope = mock(Envelope.class);
        when(envelope.getDeliveryTag()).thenReturn(deliveryTag);

        doThrow(new RejectedExecutionException()).when(mockExecutorService).submit(any(Runnable.class));

        try {
            amqpMessageConsumer.handleDelivery("consumer tag", envelope, null, "some message".getBytes());
            verify(mockChannel).basicReject(deliveryTag, REQUEUE_REJECTED_MESSAGES);
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testRejectFailed() throws IOException {
        long deliveryTag = 1234L;
        Envelope envelope = mock(Envelope.class);
        when(envelope.getDeliveryTag()).thenReturn(deliveryTag);

        doThrow(new RejectedExecutionException()).when(mockExecutorService).submit(any(Runnable.class));
        doThrow(new RuntimeException()).when(mockChannel).basicReject(eq(deliveryTag), anyBoolean());

        try {
            amqpMessageConsumer.handleDelivery("consumer tag", envelope, null, "some message".getBytes());
            verify(mockChannel).basicReject(eq(deliveryTag), anyBoolean());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testProperCharsetUsed() throws IOException {
        when(mockBrokerConfig.getCharset()).thenReturn(Charset.forName("UTF-32"));

        byte[] encodedMessage = new byte[] { 0, 0, 0, -10 }; // In UTF-32 ö is mapped to 000000f6
        String expectedDecodedMessage = "ö";

        Envelope envelope = mock(Envelope.class);
        when(envelope.getDeliveryTag()).thenReturn(1234L);

        AmqpMessageConsumer consumer = new AmqpMessageConsumer(mockChannel, mockExecutorService, mockMessageHandler, mockBrokerConfig);
        consumer.handleDelivery("some tag", envelope, null, encodedMessage);

        ArgumentCaptor<AmqpMessageProcessingTask> taskCaptor = ArgumentCaptor.forClass(AmqpMessageProcessingTask.class);
        verify(mockExecutorService).submit(taskCaptor.capture());
        assertEquals(expectedDecodedMessage, taskCaptor.getValue().body);
    }

}