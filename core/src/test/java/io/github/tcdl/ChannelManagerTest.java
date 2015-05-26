package io.github.tcdl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import io.github.tcdl.adapters.mock.AdapterFactory;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.Event;
import io.github.tcdl.messages.Message;
import io.github.tcdl.support.TestUtils;

import javax.xml.ws.Holder;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by rdro on 4/24/2015.
 */
public class ChannelManagerTest {

    private MsbMessageOptions config;
    private ChannelManager channelManager;
    private MsbConfigurations msbConfig;

    @Before
    public void setUp() {
        this.config = TestUtils.createSimpleConfig();
        this.msbConfig = TestUtils.createMsbConfigurations();
        this.channelManager = new ChannelManager(new AdapterFactory(msbConfig));
    }

    @After
    public void cleanUp() {
        channelManager.removeProducer(config.getNamespace());
    }

    @Test
    public void test_createOrFindProducer() {
        final Holder<Boolean> newProducerTopicEventFired = new Holder<>();
        final Holder<String> topicName = new Holder<>();

        channelManager.on(Event.PRODUCER_NEW_TOPIC_EVENT, (String topic) -> {
                newProducerTopicEventFired.value = true;
                topicName.value = topic;
        });

        String topic = config.getNamespace();
        channelManager.removeProducer(topic);
        Producer producer = channelManager.findOrCreateProducer(topic);

        assertNotNull(producer);
        assertTrue(newProducerTopicEventFired.value);
        assertEquals(topic, topicName.value);
    }

    @Test
    public void test_createOrFindConsumer() throws Exception {
        final Holder<Boolean> newConsumerTopicFired = new Holder<>();
        final Holder<String> topicName = new Holder<>();

        channelManager.on(Event.CONSUMER_NEW_TOPIC_EVENT, (String topic) -> {
                newConsumerTopicFired.value = true;
                topicName.value = topic;
        });

        String topic = config.getNamespace();
        channelManager.removeConsumer(topic);
        Consumer consumer = channelManager.findOrCreateConsumer(topic, msbConfig);

        assertNotNull(consumer);
        assertTrue(newConsumerTopicFired.value);
        assertEquals(topic, topicName.value);
    }

    @Test
    public void test_removeConsumer() {
        final Holder<Boolean> consumerRemovedTopicEventFired = new Holder<>();
        final Holder<String> topicName = new Holder<>();

        channelManager.on(Event.CONSUMER_REMOVED_TOPIC_EVENT, (String topic) -> {
                consumerRemovedTopicEventFired.value = true;
                topicName.value = topic;
        });

        String topic = config.getNamespace();
        channelManager.findOrCreateConsumer(topic, msbConfig);
        channelManager.removeConsumer(topic);

        assertTrue(consumerRemovedTopicEventFired.value);
        assertEquals(topic, topicName.value);
    }

    @Test
    public void test_publishMessage() {
        final Holder<Boolean> producerNewMessageEventFired = new Holder<>();
        final Holder<String> topicName = new Holder<>();

        channelManager.on(Event.PRODUCER_NEW_MESSAGE_EVENT, (String topic) -> {
                producerNewMessageEventFired.value = true;
                topicName.value = topic;
        });

        String topic = config.getNamespace();
        Producer producer = channelManager.findOrCreateProducer(topic);
        Message message = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(topic);
        producer.publish(message, null);

        assertTrue(producerNewMessageEventFired.value);
        assertEquals(topic, topicName.value);
    }

    @Test
    public void test_receiveMessage() {
        final Holder<Boolean> consumerNewMessageEventFired = new Holder<>();
        final Holder<String> topicName = new Holder<>();
        final Holder<Message> messageEvent = new Holder<>();

        channelManager.on(Event.CONSUMER_NEW_MESSAGE_EVENT, (String topic) -> {
                consumerNewMessageEventFired.value = true;
                topicName.value = topic;
        });

        channelManager.on(Event.MESSAGE_EVENT,(Message message) -> {
                messageEvent.value = message;
        });

        String topic = config.getNamespace();

        Message message = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(topic);
        channelManager.findOrCreateProducer(config.getNamespace()).publish(message, null);
        channelManager.findOrCreateConsumer(topic, msbConfig);

        assertTrue(consumerNewMessageEventFired.value);
        assertEquals(topic, topicName.value);
        assertNotNull(messageEvent.value);
    }
}
