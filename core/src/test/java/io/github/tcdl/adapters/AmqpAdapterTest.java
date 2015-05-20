package io.github.tcdl.adapters;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import io.github.tcdl.config.AmqpBrokerConfig;
import io.github.tcdl.config.MsbConfigurations;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static io.github.tcdl.adapters.Adapter.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ AmqpConnectionManager.class, MsbConfigurations.class })
public class AmqpAdapterTest {

    private Channel mockChannel;
    private MsbConfigurations mockConfiguration;

    @Before
    public void setUp() throws IOException {
        // Setup channel mock
        mockStatic(AmqpConnectionManager.class);
        Connection mockConnection = mock(Connection.class);
        mockChannel = mock(Channel.class);
        when(AmqpConnectionManager.obtainConnection()).thenReturn(mockConnection);
        when(mockConnection.createChannel()).thenReturn(mockChannel);

        // Setup configuration mock
        mockConfiguration = mock(MsbConfigurations.class);
    }

    @Test
    public void testTopicExchangeCreated() throws IOException {
        String topicName = "myTopic";

        new AmqpAdapter(topicName, mockConfiguration);

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
        verify(mockChannel).basicConsume(eq("myTopic.myGroupId.t"), amqpConsumerCaptor.capture());

        // verify that when message arrives, AMQP handler invokes ours
        amqpConsumerCaptor.getValue().handleDelivery("consumer tag", null, null, "some message".getBytes());
        verify(mockHandler).onMessage("some message");
    }

    @Test
    public void testUnsubscribe() throws IOException {
        AmqpAdapter adapter = createAdapterForSubscribe("myTopic", "myGroupId", false);
        String consumerTag = "my consumer tag";
        when(mockChannel.basicConsume(anyString(), any(Consumer.class))).thenReturn(consumerTag);

        adapter.subscribe(jsonMessage -> {
        });
        adapter.unsubscribe();

        verify(mockChannel).basicCancel(consumerTag);
    }

    private AmqpAdapter createAdapterForSubscribe(String topic, String groupId, boolean durable) {
        AmqpBrokerConfig nondurableAmqpConfig = new AmqpBrokerConfig("127.0.0.1", 10, groupId, durable);
        when(mockConfiguration.getAmqpBrokerConf()).thenReturn(nondurableAmqpConfig);

        return new AmqpAdapter(topic, mockConfiguration);
    }
}