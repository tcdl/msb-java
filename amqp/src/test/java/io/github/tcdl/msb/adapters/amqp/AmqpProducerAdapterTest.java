package io.github.tcdl.msb.adapters.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import io.github.tcdl.msb.api.exception.ChannelException;
import io.github.tcdl.msb.config.amqp.AmqpBrokerConfig;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.AdditionalMatchers;

import java.io.IOException;
import java.nio.charset.Charset;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AmqpProducerAdapterTest {
    private Channel mockChannel;
    private AmqpConnectionManager mockAmqpConnectionManager;
    private AmqpBrokerConfig mockAmqpBrokerConfig;

    @Before
    public void setUp() throws IOException {
        Connection mockConnection = mock(Connection.class);
        mockChannel = mock(Channel.class);
        mockAmqpBrokerConfig = mock(AmqpBrokerConfig.class);
        when(mockAmqpBrokerConfig.getCharset()).thenReturn(Charset.forName("UTF-8"));

        mockAmqpConnectionManager = mock(AmqpConnectionManager.class);

        when(mockAmqpConnectionManager.obtainConnection()).thenReturn(mockConnection);
        when(mockConnection.createChannel()).thenReturn(mockChannel);
    }

    @Test
    public void testExchangeCreated() throws IOException {
        String topicName = "myTopic";

        new AmqpProducerAdapter(topicName, mockAmqpBrokerConfig, mockAmqpConnectionManager);

        verify(mockChannel).exchangeDeclare(topicName, "fanout", false, true, null);
    }

    @Test(expected = RuntimeException.class)
    public void testInitializationError() throws IOException {
        when(mockChannel.exchangeDeclare(anyString(), anyString(), anyBoolean(), anyBoolean(), any())).thenThrow(new IOException());
        new AmqpProducerAdapter("myTopic", mockAmqpBrokerConfig, mockAmqpConnectionManager);
    }

    @Test
    public void testPublish() throws ChannelException, IOException {
        String topicName = "myTopic";
        String message = "message";
        AmqpProducerAdapter producerAdapter = new AmqpProducerAdapter(topicName, mockAmqpBrokerConfig, mockAmqpConnectionManager);

        producerAdapter.publish(message);

        verify(mockChannel).basicPublish(topicName, StringUtils.EMPTY, MessageProperties.PERSISTENT_BASIC, message.getBytes());
    }

    @Test
    public void testPublishWithRoutingKey() throws Exception{
        String topicName = "myTopic";
        String message = "message";
        String routingKey = "routingKey";
        AmqpProducerAdapter producerAdapter = new AmqpProducerAdapter(topicName, mockAmqpBrokerConfig, mockAmqpConnectionManager);

        producerAdapter.publish(message, routingKey);
        verify(mockChannel).basicPublish(topicName, routingKey, MessageProperties.PERSISTENT_BASIC, message.getBytes());
    }

    @Test
    public void testProperCharsetUsed() throws IOException {
        when(mockAmqpBrokerConfig.getCharset()).thenReturn(Charset.forName("UTF-32"));

        String message = "ö";
        byte[] expectedEncodedMessage = new byte[] { 0, 0, 0, -10 }; // In UTF-32 ö is mapped to 000000f6
        AmqpProducerAdapter producerAdapter = new AmqpProducerAdapter("myTopic", mockAmqpBrokerConfig, mockAmqpConnectionManager);

        producerAdapter.publish(message);

        verify(mockChannel).basicPublish(anyString(), anyString(), any(AMQP.BasicProperties.class), AdditionalMatchers.aryEq(expectedEncodedMessage));
    }
}