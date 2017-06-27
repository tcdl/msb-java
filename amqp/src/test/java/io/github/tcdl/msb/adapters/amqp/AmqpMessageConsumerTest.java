package io.github.tcdl.msb.adapters.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerImpl;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.config.amqp.AmqpBrokerConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.RejectedExecutionException;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AmqpMessageConsumerTest {

    @Mock
    private Channel mockChannel;

    @Mock
    private ConsumerAdapter.RawMessageHandler mockMessageHandler;

    @Mock
    private AmqpBrokerConfig mockBrokerConfig;

    @Mock
    private AcknowledgementHandlerImpl amqpAcknowledgementHandler;

    private AmqpMessageConsumer amqpMessageConsumer;

    @Before
    public void setUp() {
        when(mockBrokerConfig.getCharset()).thenReturn(Charset.forName("UTF-8"));

        amqpMessageConsumer = new AmqpMessageConsumer(mockChannel, mockMessageHandler, mockBrokerConfig) {
            @Override
            AcknowledgementHandlerImpl createAcknowledgementHandler(Channel channel, String consumerTag, long deliveryTag, boolean isRequeueRejectedMessages) {
                return amqpAcknowledgementHandler;
            }
        };
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

        verify(mockMessageHandler, times(1)).onMessage(eq(messageStr), eq(amqpAcknowledgementHandler));

    }

    @Test
    public void testMessageCannotBeSubmittedForProcessing() throws IOException {
        long deliveryTag = 1234L;
        Envelope envelope = mock(Envelope.class);
        when(envelope.getDeliveryTag()).thenReturn(deliveryTag);

        doThrow(new RejectedExecutionException()).when(mockMessageHandler).onMessage(anyString(), any());

        try {
            amqpMessageConsumer.handleDelivery("consumer tag", envelope, null, "some message".getBytes());
            verify(amqpAcknowledgementHandler, times(1)).autoReject();
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testMessageCannotBeProcessedBeforeSubmit() throws IOException {
        long deliveryTag = 1234L;
        Envelope envelope = mock(Envelope.class);
        when(envelope.getDeliveryTag()).thenReturn(deliveryTag);

        when(mockBrokerConfig.getCharset()).thenThrow(
                new RuntimeException("Something really unexpected happened even before task submit attempt"));

        try {
            amqpMessageConsumer.handleDelivery("consumer tag", envelope, null, "some message".getBytes());
            verify(amqpAcknowledgementHandler, times(1)).autoReject();
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testRejectFailed() throws IOException {
        long deliveryTag = 1234L;
        Envelope envelope = mock(Envelope.class);
        when(envelope.getDeliveryTag()).thenReturn(deliveryTag);

        doThrow(new RejectedExecutionException()).when(mockMessageHandler).onMessage(anyString(), any());
        doThrow(new RuntimeException()).when(mockChannel).basicReject(eq(deliveryTag), anyBoolean());

        try {
            amqpMessageConsumer.handleDelivery("consumer tag", envelope, null, "some message".getBytes());
            verify(amqpAcknowledgementHandler, times(1)).autoReject();
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

        AmqpMessageConsumer consumer = new AmqpMessageConsumer(mockChannel, mockMessageHandler, mockBrokerConfig);
        consumer.handleDelivery("some tag", envelope, null, encodedMessage);

        verify(mockMessageHandler, times(1)).onMessage(eq(expectedDecodedMessage), any());

    }

}