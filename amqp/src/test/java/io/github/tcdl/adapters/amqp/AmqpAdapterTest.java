package io.github.tcdl.adapters.amqp;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import io.github.tcdl.adapters.Adapter.RawMessageHandler;
import io.github.tcdl.config.amqp.AmqpBrokerConfig;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ AmqpConnectionManager.class, AmqpBrokerConfig.class })
public class AmqpAdapterTest {

    private Channel mockChannel;

    @Before
    public void setUp() throws Exception {
        // Setup channel mock
        Connection mockConnection = mock(Connection.class);
        mockChannel = mock(Channel.class);

        AmqpConnectionManager mockAmqpConnectionManager = mock(AmqpConnectionManager.class);
        
        PowerMockito.mockStatic(AmqpConnectionManager.class);
        when(AmqpConnectionManager.getInstance()).thenReturn(mockAmqpConnectionManager);
        when(mockAmqpConnectionManager.obtainConnection(any(AmqpBrokerConfig.class))).thenReturn(mockConnection);

        when(mockConnection.createChannel()).thenReturn(mockChannel);
    }

    @Test
    public void testTopicExchangeCreated() throws Exception {
        String topicName = "myTopic";
        AmqpAdapter adapter = createAdapterForSubscribe(topicName, "myGroupId", false);

        adapter.subscribe(jsonMessage -> {
        });

        verify(mockChannel).exchangeDeclare(topicName, "fanout", false, true, null);
    }

    @Test
    public void testSubscribeTransientQueueCreated() throws IOException {
        AmqpAdapter adapter = createAdapterForSubscribe("myTopic", "myGroupId", false);
        
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
        AmqpAdapter adapter = createAdapterForSubscribe("myTopic", "myGroupId", true);

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
        AmqpAdapter adapter = createAdapterForSubscribe("myTopic", "myGroupId", false);
        RawMessageHandler mockHandler = mock(RawMessageHandler.class);

        adapter.subscribe(mockHandler);

        // verify that AMQP handler has been registered
        ArgumentCaptor<Consumer> amqpConsumerCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(mockChannel).basicConsume(eq("myTopic.myGroupId.t"), eq(false) /* autoAck */, amqpConsumerCaptor.capture());

        // verify that when message arrives, AMQP handler invokes ours
        long deliveryTag = 1234L;
        String messageStr = "some message";
        Envelope envelope = mock(Envelope.class);
        when(envelope.getDeliveryTag()).thenReturn(deliveryTag);
        amqpConsumerCaptor.getValue().handleDelivery("consumer tag", envelope, null, messageStr.getBytes());
        verify(mockHandler).onMessage(messageStr);

        // Check that acknowledgement for this message has been sent
        verify(mockChannel).basicAck(deliveryTag, false);
    }

    @Test
    public void testUnsubscribe() throws IOException {
        AmqpAdapter adapter = createAdapterForSubscribe("myTopic", "myGroupId", false);
        String consumerTag = "my consumer tag";
        when(mockChannel.basicConsume(anyString(), anyBoolean(), any(Consumer.class))).thenReturn(consumerTag);

        adapter.subscribe(jsonMessage -> {
        });
        adapter.unsubscribe();

        verify(mockChannel).basicCancel(consumerTag);
    }

    private AmqpAdapter createAdapterForSubscribe(String topic, String groupId, boolean durable) {
        AmqpBrokerConfig nondurableAmqpConfig = new AmqpBrokerConfig("127.0.0.1", 10, groupId, durable);
        return new AmqpAdapter(topic, nondurableAmqpConfig);
    }
}