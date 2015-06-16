package io.github.tcdl.adapters.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import io.github.tcdl.config.amqp.AmqpBrokerConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import static io.github.tcdl.adapters.ConsumerAdapter.RawMessageHandler;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AmqpConsumerAdapterTest {

    private Channel mockChannel;
    private AmqpConnectionManager mockAmqpConnectionManager;
    private ExecutorService mockConsumerThreadPool;

    @Before
    public void setUp() throws Exception {
        Connection mockConnection = mock(Connection.class);
        mockChannel = mock(Channel.class);
        mockAmqpConnectionManager = mock(AmqpConnectionManager.class);
        
        when(mockAmqpConnectionManager.obtainConnection()).thenReturn(mockConnection);
        when(mockConnection.createChannel()).thenReturn(mockChannel);

        mockConsumerThreadPool = mock(ExecutorService.class);
    }

    @Test
    public void testTopicExchangeCreated() throws Exception {
        String topicName = "myTopic";
        AmqpConsumerAdapter adapter = createAdapter(topicName, "myGroupId", false);

        adapter.subscribe(jsonMessage -> {
        });

        verify(mockChannel).exchangeDeclare(topicName, "fanout", false, true, null);
    }

    @Test(expected = RuntimeException.class)
    public void testInitializationError() throws IOException {
        when(mockChannel.exchangeDeclare(anyString(), anyString(), anyBoolean(), anyBoolean(), any())).thenThrow(new IOException());

        createAdapter("myTopic", "myGroupId", false);
    }

    @Test
    public void testSubscribeTransientQueueCreated() throws IOException {
        AmqpConsumerAdapter adapter = createAdapter("myTopic", "myGroupId", false);
        
        adapter.subscribe(jsonMessage -> {
        });

        // Verify that the queue has been declared with correct name and settings
        verify(mockChannel).queueDeclare("myTopic.myGroupId.t", /* queue name */
                false, /* durable */
                false, /* exclusive */
                true,  /* auto-delete */
                null);
        // Verify that the queue has been bound to the exchange
        verify(mockChannel).queueBind("myTopic.myGroupId.t", "myTopic", "");
    }

    @Test
    public void testSubscribeDurableQueueCreated() throws IOException {
        AmqpConsumerAdapter adapter = createAdapter("myTopic", "myGroupId", true);

        adapter.subscribe(jsonMessage -> {
        });

        // Verify that the queue has been declared with correct name and settings
        verify(mockChannel).queueDeclare("myTopic.myGroupId.d", /* queue name */
                true, /* durable */
                false, /* exclusive */
                false,  /* auto-delete */
                null);
        // Verify that the queue has been bound to the exchange
        verify(mockChannel).queueBind("myTopic.myGroupId.d", "myTopic", "");
    }

    @Test
    public void testRegisteredHandlerInvoked() throws IOException {
        AmqpConsumerAdapter adapter = createAdapter("myTopic", "myGroupId", false);
        RawMessageHandler mockHandler = mock(RawMessageHandler.class);

        adapter.subscribe(mockHandler);

        // verify that AMQP handler has been registered
        ArgumentCaptor<Consumer> amqpConsumerCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockChannel).basicConsume(eq("myTopic.myGroupId.t"), eq(false) /* autoAck */, amqpConsumerCaptor.capture());

        assertTrue(amqpConsumerCaptor.getValue() instanceof AmqpMsbConsumer);
        AmqpMsbConsumer consumer = (AmqpMsbConsumer) amqpConsumerCaptor.getValue();
        assertEquals(mockChannel, consumer.getChannel());
        assertEquals(mockConsumerThreadPool, consumer.consumerThreadPool);
        assertEquals(mockHandler, consumer.msgHandler);
    }

    @Test
    public void testUnsubscribe() throws IOException {
        AmqpConsumerAdapter adapter = createAdapter("myTopic", "myGroupId", false);
        String consumerTag = "my consumer tag";
        when(mockChannel.basicConsume(anyString(), anyBoolean(), any(Consumer.class))).thenReturn(consumerTag);

        adapter.subscribe(jsonMessage -> {
        });
        adapter.unsubscribe();

        verify(mockChannel).basicCancel(consumerTag);
    }

    private AmqpConsumerAdapter createAdapter(String topic, String groupId, boolean durable) {
        AmqpBrokerConfig nondurableAmqpConfig = new AmqpBrokerConfig("127.0.0.1", 10, null, null, null, groupId, durable, 5);
        return new AmqpConsumerAdapter(topic, nondurableAmqpConfig, mockAmqpConnectionManager, mockConsumerThreadPool);
    }
}