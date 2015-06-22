package io.github.tcdl;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import javax.xml.ws.Holder;
import java.time.Clock;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.messages.Message;
import io.github.tcdl.monitor.ChannelMonitorAgent;
import io.github.tcdl.support.JsonValidator;
import io.github.tcdl.support.TestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * @author rdro
 * @since 4/24/2015
 */
public class ChannelManagerTest {

    private ChannelManager channelManager;
    private ChannelMonitorAgent mockChannelMonitorAgent;
    private Subscriber subscriberMock;

    @Before
    public void setUp() {
        MsbConfigurations msbConfig = TestUtils.createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();
        JsonValidator validator = new JsonValidator();
        this.channelManager = new ChannelManager(msbConfig, clock, validator);

        mockChannelMonitorAgent = mock(ChannelMonitorAgent.class);
        channelManager.setChannelMonitorAgent(mockChannelMonitorAgent);
        subscriberMock = mock(Subscriber.class);
    }

    @Test
    public void testProducerCached() {
        String topic = "topic:test-producer-cached";

        // Producer was created and monitor agent notified
        Producer producer1 = channelManager.findOrCreateProducer(topic);
        assertNotNull(producer1);
        verify(mockChannelMonitorAgent).producerTopicCreated(topic);

        // Cached producer was returned and monitor agent wasn't notified
        Producer producer2 = channelManager.findOrCreateProducer(topic);
        assertNotNull(producer2);
        assertSame(producer1, producer2);
        verifyNoMoreInteractions(mockChannelMonitorAgent);
    }

    @Test
    public void testConsumerCached() throws Exception {
        String topic = "topic:test-consumer-cached";

        // Consumer was created and monitor agent notified
        channelManager.subscribe(topic, subscriberMock);
        verify(mockChannelMonitorAgent).consumerTopicCreated(topic);

        // Cached consumer was returned and monitor agent wasn't notified
        channelManager.subscribe(topic, subscriberMock);
        verifyNoMoreInteractions(mockChannelMonitorAgent);
    }

    @Test
    public void testPublishMessageInvokesAgent() {
        String topic = "topic:test-agent-publish";

        Producer producer = channelManager.findOrCreateProducer(topic);
        Message message = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(topic);
        producer.publish(message);

        verify(mockChannelMonitorAgent).producerMessageSent(topic);
    }

    @Test
    public void testReceiveMessageInvokesAgentAndEmitsEvent() throws InterruptedException {
        String topic = "topic:test-agent-consume";

        CountDownLatch awaitReceiveEvents = new CountDownLatch(1);
        final Holder<Message> messageEvent = new Holder<>();

        Message message = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(topic);
        channelManager.findOrCreateProducer(topic).publish(message);
        channelManager.subscribe(topic,
                msg ->  {
                    messageEvent.value = msg;
                    awaitReceiveEvents.countDown();
                });

        assertTrue(awaitReceiveEvents.await(4000, TimeUnit.MILLISECONDS));
        verify(mockChannelMonitorAgent).consumerMessageReceived(topic);
        assertNotNull(messageEvent.value);
    }


    @Test
    public void testSubscribeSubscribeMultiple() {
        String topic = "topic:test-unsubscribe-multi";
        CollectorSubscriber collectorSubscriber1 = new CollectorSubscriber(channelManager);
        CollectorSubscriber collectorSubscriber2 = new CollectorSubscriber(channelManager);

        Consumer consumer1 = channelManager.subscribe(topic, collectorSubscriber1);
        Consumer consumer2 = channelManager.subscribe(topic, collectorSubscriber2);

        assertTrue(consumer1.equals(consumer2));
    }


    @Test
    public void testSubscribeUnsubscribe() {
        String topic = "topic:test-unsubscribe-once";
        CollectorSubscriber collectorSubscriber = new CollectorSubscriber(channelManager);

        channelManager.subscribe(topic, collectorSubscriber);
        channelManager.unsubscribe(topic);

        verify(mockChannelMonitorAgent).consumerTopicRemoved(topic);
    }


    @Test
    public void testSubscribeUnsubscribeSeparateTopics() {
        String topic1 = "topic:test-unsubscribe-try-first";
        String topic2 = "topic:test-unsubscribe-try-other";
        CollectorSubscriber collectorSubscriber = new CollectorSubscriber(channelManager);

        channelManager.subscribe(topic1,collectorSubscriber);
        channelManager.subscribe(topic2, collectorSubscriber);

        channelManager.unsubscribe(topic1);
        verify(mockChannelMonitorAgent).consumerTopicRemoved(topic1);
        verify(mockChannelMonitorAgent, never()).consumerTopicRemoved(topic2);
    }

}