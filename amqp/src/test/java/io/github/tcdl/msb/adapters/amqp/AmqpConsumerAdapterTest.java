package io.github.tcdl.msb.adapters.amqp;

import com.google.common.collect.Sets;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.api.exception.ChannelException;
import io.github.tcdl.msb.config.amqp.AmqpBrokerConfig;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class AmqpConsumerAdapterTest {

    private Channel mockChannel;
    private AmqpConnectionManager mockAmqpConnectionManager;


    @Before
    public void setUp() throws Exception {
        Connection mockConnection = mock(Connection.class);
        mockChannel = mock(Channel.class);
        mockAmqpConnectionManager = mock(AmqpConnectionManager.class);
        
        when(mockAmqpConnectionManager.obtainConnection()).thenReturn(mockConnection);
        when(mockConnection.createChannel()).thenReturn(mockChannel);
    }

    @Test
    public void testFanoutExchangeCreated() throws Exception {
        String topicName = "myTopic";
        String groupId = "groupId";
        AmqpConsumerAdapter adapter = createAdapterWithNonDurableConf(topicName, groupId, false);

        adapter.subscribe((jsonMessage, ackHandler) -> {
        });

        verify(mockChannel).exchangeDeclare(topicName, "fanout", false, true, null);
    }

    @Test
    public void testTopicExchangeCreated() throws Exception {
        String topicName = "myTopic";
        String groupId = "groupId";
        String routingKey = "routing-key";

        new AmqpConsumerAdapter(topicName, Collections.singleton(routingKey), brokerConfig(groupId, true), mockAmqpConnectionManager);
        verify(mockChannel).exchangeDeclare(topicName, "topic", false, true, null);

    }

    @Test(expected = RuntimeException.class)
    public void testInitializationError() throws IOException {
        when(mockChannel.exchangeDeclare(anyString(), anyString(), anyBoolean(), anyBoolean(), any())).thenThrow(new IOException());

        createAdapterWithNonDurableConf("myTopic", "myGroupId", false);
    }

    @Test
    public void testSubscribeMultipleRoutingKeysMultipleBindings() throws Exception {
        String topicName = "myTopic";
        String groupId = "groupId";

        Set<String> routingKeys = Sets.newHashSet("routing-key-1", "routing-key-2");

        AmqpConsumerAdapter amqpConsumerAdapter = new AmqpConsumerAdapter(topicName, routingKeys, brokerConfig(groupId, true), mockAmqpConnectionManager);
        amqpConsumerAdapter.subscribe((jsonMessage, acknowledgementHandler) -> {});

        ArgumentCaptor<String> routingKeysCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockChannel, times(2)).queueBind(eq("myTopic.groupId.d"), eq(topicName), routingKeysCaptor.capture());

        assertTrue(CollectionUtils.isEqualCollection(routingKeys, routingKeysCaptor.getAllValues()));
    }

    @Test
    public void testSubscribeTransientQueueCreated() throws IOException {
        AmqpConsumerAdapter adapter = createAdapterWithNonDurableConf("myTopic", "myGroupId", false);
        
        adapter.subscribe((jsonMessage, ackHandler) -> {
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
    public void testSubscribeTransientQueueCreatedWhenIsResponseTopic() throws IOException {
        AmqpConsumerAdapter adapter = createAdapterWithDurableConf("myTopic", "myGroupId", true);

        adapter.subscribe((jsonMessage, ackHandler) -> {
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
        AmqpConsumerAdapter adapter = createAdapterWithDurableConf("myTopic", "myGroupId", false);

        adapter.subscribe((jsonMessage, ackHandler) -> {
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


    @Test(expected = ChannelException.class)
    public void testSubscribeException() throws IOException {
        AmqpConsumerAdapter adapter = createAdapterWithDurableConf("myTopic", "myGroupId", false);
        when(mockChannel.basicConsume(anyString(), anyBoolean(), any(AmqpMessageConsumer.class)))
                .thenThrow(IOException.class);
        adapter.subscribe((jsonMessage, ackHandler) -> {});
    }

    @Test
    public void testRegisteredHandlerInvoked() throws IOException {
        AmqpConsumerAdapter adapter = createAdapterWithNonDurableConf("myTopic", "myGroupId", false);
        ConsumerAdapter.RawMessageHandler mockHandler = mock(ConsumerAdapter.RawMessageHandler.class);

        adapter.subscribe(mockHandler);

        // verify that AMQP handler has been registered
        ArgumentCaptor<AmqpMessageConsumer> amqpConsumerCaptor = ArgumentCaptor.forClass(AmqpMessageConsumer.class);
        verify(mockChannel).basicConsume(eq("myTopic.myGroupId.t"), eq(false) /* autoAck */, amqpConsumerCaptor.capture());

        AmqpMessageConsumer consumer = amqpConsumerCaptor.getValue();
        assertEquals(mockChannel, consumer.getChannel());
        assertEquals(mockHandler, consumer.msgHandler);
    }

    @Test
    public void testUnsubscribe() throws IOException {
        AmqpConsumerAdapter adapter = createAdapterWithNonDurableConf("myTopic", "myGroupId", false);
        String consumerTag = "my consumer tag";
        when(mockChannel.basicConsume(anyString(), anyBoolean(), any(Consumer.class))).thenReturn(consumerTag);

        adapter.subscribe((jsonMessage, ackHandler) -> {
        });
        adapter.unsubscribe();

        verify(mockChannel).basicCancel(consumerTag);
    }

    @Test(expected = ChannelException.class)
    public void testUnsubscribeException() throws IOException {
        AmqpConsumerAdapter adapter = createAdapterWithNonDurableConf("myTopic", "myGroupId", false);
        String consumerTag = "my consumer tag";
        when(mockChannel.basicConsume(anyString(), anyBoolean(), any(Consumer.class)))
                .thenThrow(IOException.class);
        adapter.subscribe((jsonMessage, ackHandler) -> {});
        adapter.unsubscribe();
    }

    @Test
    public void testIsDurableFalseIfResponseTopicAndNonDurableConfig() throws IOException {
        boolean isResponseTopic = true;

        AmqpConsumerAdapter adapter = createAdapterWithNonDurableConf("myTopic", "myGroupId", isResponseTopic);

        assertFalse(adapter.isDurable());
    }

    @Test
    public void testIsDurableFalseIfNotResponseTopicAndNonDurableConfig() throws IOException {
        boolean isResponseTopic = false;

        AmqpConsumerAdapter adapter = createAdapterWithNonDurableConf("myTopic", "myGroupId", isResponseTopic);

        assertFalse(adapter.isDurable());
    }

    @Test
    public void testIsDurableFalseIfResponseTopicAndDurableConfig() throws IOException {
        boolean isResponseTopic = true;

        AmqpConsumerAdapter adapter = createAdapterWithDurableConf("myTopic", "myGroupId", isResponseTopic);

        assertFalse(adapter.isDurable());
    }

    @Test
    public void testIsDurableTrueIfNotResponseTopicAndDurableConfig() throws IOException {
        boolean isResponseTopic = false;

        AmqpConsumerAdapter adapter = createAdapterWithDurableConf("myTopic", "myGroupId", isResponseTopic);

        assertTrue(adapter.isDurable());
    }

    private AmqpConsumerAdapter createAdapterWithNonDurableConf(String topic, String groupId, boolean isResponseTopic) {
        boolean isDurableConf = false;
        AmqpBrokerConfig nondurableAmqpConfig = new AmqpBrokerConfig(Charset.forName("UTF-8"), "127.0.0.1", 10, Optional.empty(), Optional.empty(), Optional.empty(),
                false, Optional.of(groupId), isDurableConf, 1, 5000, 1);
        return new AmqpConsumerAdapter(topic, nondurableAmqpConfig, mockAmqpConnectionManager, isResponseTopic);
    }

    private AmqpConsumerAdapter createAdapterWithDurableConf(String topic, String groupId, boolean isResponseTopic) {
        boolean isDurableConf = true;
        AmqpBrokerConfig nondurableAmqpConfig = new AmqpBrokerConfig(Charset.forName("UTF-8"), "127.0.0.1", 10, Optional.empty(), Optional.empty(), Optional.empty(),
                false, Optional.of(groupId), isDurableConf, 1, 5000, 1);
        return new AmqpConsumerAdapter(topic, nondurableAmqpConfig, mockAmqpConnectionManager, isResponseTopic);
    }

    private AmqpBrokerConfig brokerConfig(String groupId, boolean durable) {
        return new AmqpBrokerConfig(
                Charset.forName("UTF-8"),
                "127.0.0.1", 10, Optional.empty(), Optional.empty(), Optional.empty(),
                false, Optional.of(groupId), durable, 1, 5000, 1
        );
    }
}