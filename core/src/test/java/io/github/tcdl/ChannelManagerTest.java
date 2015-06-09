package io.github.tcdl;

import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.messages.Message;
import io.github.tcdl.monitor.ChannelMonitorAgent;
import io.github.tcdl.support.TestUtils;
import org.junit.Before;
import org.junit.Test;

import javax.xml.ws.Holder;
import java.time.Clock;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * @author rdro
 * @since 4/24/2015
 */
public class ChannelManagerTest {

    private ChannelManager channelManager;
    private ChannelMonitorAgent mockChannelMonitorAgent;

    @Before
    public void setUp() {
        MsbConfigurations msbConfig = TestUtils.createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();
        this.channelManager = new ChannelManager(msbConfig, clock);

        mockChannelMonitorAgent = mock(ChannelMonitorAgent.class);
        channelManager.setChannelMonitorAgent(mockChannelMonitorAgent);
    }

    @Test
    public void testProducerCached() {
        String topic = "topic:test";

        // Producer was created and monitor agent notified
        Producer producer1 = channelManager.findOrCreateProducer(topic);
        assertNotNull(producer1);
        verify(mockChannelMonitorAgent).producerTopicCreated(topic);

        // Cached producer was removed and monitor agent wasn't notified
        Producer producer2 = channelManager.findOrCreateProducer(topic);
        assertNotNull(producer2);
        assertSame(producer1, producer2);
        verifyNoMoreInteractions(mockChannelMonitorAgent);
    }

    @Test
    public void testConsumerCached() throws Exception {
        String topic = "topic:test";

        Consumer consumer1 = channelManager.findOrCreateConsumer(topic);
        assertNotNull(consumer1);
        verify(mockChannelMonitorAgent).consumerTopicCreated(topic);

        Consumer consumer2 = channelManager.findOrCreateConsumer(topic);
        assertNotNull(consumer2);
        assertSame(consumer1, consumer2);
        verifyNoMoreInteractions(mockChannelMonitorAgent);
    }

    @Test
    public void testRemoveConsumer() {
        String topic = "topic:test";

        channelManager.removeConsumer(topic);
        verify(mockChannelMonitorAgent, never()).consumerTopicRemoved(topic);

        channelManager.findOrCreateConsumer(topic); // force creation of the consumer
        channelManager.removeConsumer(topic);
        verify(mockChannelMonitorAgent).consumerTopicRemoved(topic);
    }

    @Test
    public void testPublishMessageInvokesAgent() {
        String topic = "topic:test";

        Producer producer = channelManager.findOrCreateProducer(topic);
        Message message = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(topic);
        producer.publish(message);

        verify(mockChannelMonitorAgent).producerMessageSent(topic);
    }

    @Test
    public void testReceiveMessageInvokesAgentAndEmitsEvent() throws InterruptedException {
        String topic = "topic:test";

        CountDownLatch awaitReceiveEvents = new CountDownLatch(1);
        final Holder<Message> messageEvent = new Holder<>();

        Message message = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(topic);
        channelManager.findOrCreateProducer(topic).publish(message);
        channelManager.findOrCreateConsumer(topic)
                .subscribe(new Consumer.Subscriber(msg -> {
                    messageEvent.value = msg;
                    awaitReceiveEvents.countDown();
                }, null, null));

        assertTrue(awaitReceiveEvents.await(3000, TimeUnit.MILLISECONDS));
        verify(mockChannelMonitorAgent).consumerMessageReceived(topic);
        assertNotNull(messageEvent.value);
    }
}
