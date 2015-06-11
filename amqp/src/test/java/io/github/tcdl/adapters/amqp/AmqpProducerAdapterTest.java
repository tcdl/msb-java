package io.github.tcdl.adapters.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import io.github.tcdl.exception.ChannelException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AmqpProducerAdapterTest {
    private Channel mockChannel;
    private AmqpConnectionManager mockAmqpConnectionManager;

    @Before
    public void setUp() throws IOException {
        Connection mockConnection = mock(Connection.class);
        mockChannel = mock(Channel.class);
        mockAmqpConnectionManager = mock(AmqpConnectionManager.class);

        when(mockAmqpConnectionManager.obtainConnection()).thenReturn(mockConnection);
        when(mockConnection.createChannel()).thenReturn(mockChannel);
    }

    @Test
    public void testExchangeCreated() throws IOException {
        String topicName = "myTopic";

        new AmqpProducerAdapter(topicName, mockAmqpConnectionManager);

        verify(mockChannel).exchangeDeclare(topicName, "fanout", false, true, null);
    }

    @Test(expected = RuntimeException.class)
    public void testInitializationError() throws IOException {
        when(mockChannel.exchangeDeclare(anyString(), anyString(), anyBoolean(), anyBoolean(), any())).thenThrow(new IOException());

        new AmqpProducerAdapter("myTopic", mockAmqpConnectionManager);
    }

    @Test
    public void testPublish() throws ChannelException, IOException {
        String topicName = "myTopic";
        String message = "message";
        AmqpProducerAdapter producerAdapter = new AmqpProducerAdapter(topicName, mockAmqpConnectionManager);

        producerAdapter.publish(message);

        verify(mockChannel).basicPublish(topicName, "" /* routing key */, MessageProperties.PERSISTENT_BASIC, message.getBytes());
    }

}