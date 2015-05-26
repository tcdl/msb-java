package io.github.tcdl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.Event;
import io.github.tcdl.messages.Message;
import io.github.tcdl.support.TestUtils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
        this.channelManager = new ChannelManager(msbConfig);
    }

    @After
    public void cleanUp() {
        channelManager.removeProducer(config.getNamespace());
    }

    @Test
    public void testFindOrCreateProducer() throws InterruptedException {
        CountDownLatch producerNewTopicEventFired = new CountDownLatch(1);
        final Holder<String> topicName = new Holder<>();

        channelManager.on(Event.PRODUCER_NEW_TOPIC_EVENT, (String topic) -> {
            producerNewTopicEventFired.countDown();
            topicName.value = topic;
        });

        String topic = config.getNamespace();
        channelManager.removeProducer(topic);
        Producer producer = channelManager.findOrCreateProducer(topic);

        assertNotNull(producer);
        assertTrue(producerNewTopicEventFired.await(100, TimeUnit.MILLISECONDS));
        assertEquals(topic, topicName.value);
    }

    @Test
    public void testFindOrCreateConsumer() throws Exception {
        CountDownLatch consumerNewTopicEventFired = new CountDownLatch(1);
        final Holder<String> topicName = new Holder<>();

        channelManager.on(Event.CONSUMER_NEW_TOPIC_EVENT, (String topic) -> {
            consumerNewTopicEventFired.countDown();
            topicName.value = topic;
        });

        String topic = config.getNamespace();
        channelManager.removeConsumer(topic);
        Consumer consumer = channelManager.findOrCreateConsumer(topic);

        assertNotNull(consumer);
        assertTrue(consumerNewTopicEventFired.await(100, TimeUnit.MILLISECONDS));
        assertEquals(topic, topicName.value);
    }

    @Test
    public void testRemoveConsumer() throws InterruptedException {
        CountDownLatch consumerRemovedTopicEventFired = new CountDownLatch(1);      
        final Holder<String> topicName = new Holder<>();

        channelManager.on(Event.CONSUMER_REMOVED_TOPIC_EVENT, (String topic) -> {
            consumerRemovedTopicEventFired.countDown();
            topicName.value = topic;
        });

        String topic = config.getNamespace();
        channelManager.findOrCreateConsumer(topic);
        channelManager.removeConsumer(topic);

        assertTrue(consumerRemovedTopicEventFired.await(100, TimeUnit.MILLISECONDS));
        assertEquals(topic, topicName.value);
    }

    @Test
    public void testProducerNewMessageEventOnPublishMessage() throws InterruptedException {
        CountDownLatch producerNewMessageEventFired = new CountDownLatch(1); 
        final Holder<String> topicName = new Holder<>();

        channelManager.on(Event.PRODUCER_NEW_MESSAGE_EVENT, (String topic) -> {
            producerNewMessageEventFired.countDown();
            topicName.value = topic;
        });

        String topic = config.getNamespace();
        Producer producer = channelManager.findOrCreateProducer(topic);
        Message message = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(topic);
        producer.publish(message, null);

        assertTrue(producerNewMessageEventFired.await(100, TimeUnit.MILLISECONDS));
        assertEquals(topic, topicName.value);
    }


    @Test
    public void testConsumerNewMessageEventOnReceiveMessage() throws InterruptedException {
        CountDownLatch awaitRecieveEvents = new CountDownLatch(2);
        final Holder<Boolean> consumerNewMessageEventFired = new Holder<>();
        final Holder<String> topicName = new Holder<>();
        final Holder<Message> messageEvent = new Holder<>();

        channelManager.on(Event.CONSUMER_NEW_MESSAGE_EVENT, (String topic) -> {
            consumerNewMessageEventFired.value = true;
            topicName.value = topic;
            awaitRecieveEvents.countDown();
        });

        channelManager.on(Event.MESSAGE_EVENT, (Message message) -> {
            messageEvent.value = message;
            awaitRecieveEvents.countDown();
        });

        String topic = config.getNamespace();

        Message message = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(topic);
        channelManager.findOrCreateProducer(config.getNamespace()).publish(message, null);
        channelManager.findOrCreateConsumer(topic);

        assertTrue(awaitRecieveEvents.await(3000, TimeUnit.MILLISECONDS));
        assertTrue(consumerNewMessageEventFired.value);
        assertEquals(topic, topicName.value);
        assertNotNull(messageEvent.value);
    }
}
