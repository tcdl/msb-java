package io.github.tcdl.msb;

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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.api.exception.ConsumerSubscriptionException;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.monitor.agent.ChannelMonitorAgent;
import io.github.tcdl.msb.support.JsonValidator;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class ChannelManagerTest {

    private ChannelManager channelManager;
    private ChannelMonitorAgent mockChannelMonitorAgent;

    @Before
    public void setUp() {
        MsbConfig msbConfig = TestUtils.createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();
        JsonValidator validator = new JsonValidator();
        ObjectMapper messageMapper = TestUtils.createMessageMapper();
        this.channelManager = new ChannelManager(msbConfig, clock, validator, messageMapper);

        mockChannelMonitorAgent = mock(ChannelMonitorAgent.class);
        channelManager.setChannelMonitorAgent(mockChannelMonitorAgent);
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

    @Test(expected = ConsumerSubscriptionException.class)
    public void testConsumerSubscribeMultipleSameTopic() {
        String topic = "topic:test-consumer-cached";

        // Consumer was created and monitor agent notified
        channelManager.subscribe(topic, message -> {});
        channelManager.subscribe(topic, message -> {});
    }

    @Test
    public void testPublishMessageInvokesAgent() {
        String topic = "topic:test-agent-publish";

        Producer producer = channelManager.findOrCreateProducer(topic);
        Message message = TestUtils.createMsbRequestMessageWithSimplePayload(topic);
        producer.publish(message);

        verify(mockChannelMonitorAgent).producerMessageSent(topic);
    }

    @Test
    public void testReceiveMessageInvokesAgentAndEmitsEvent() throws InterruptedException {
        String topic = "topic:test-agent-consume";

        CountDownLatch awaitReceiveEvents = new CountDownLatch(1);
        final Holder<Message> messageEvent = new Holder<>();

        Message message = TestUtils.createMsbRequestMessageWithSimplePayload(topic);
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
    public void testSubscribeUnsubscribe() {
        String topic = "topic:test-unsubscribe-once";

        channelManager.subscribe(topic,  message -> {});
        channelManager.unsubscribe(topic);

        verify(mockChannelMonitorAgent).consumerTopicRemoved(topic);
    }

    @Test
    public void testSubscribeUnsubscribeSeparateTopics() {
        String topic1 = "topic:test-unsubscribe-try-first";
        String topic2 = "topic:test-unsubscribe-try-other";

        channelManager.subscribe(topic1, message -> {});
        channelManager.subscribe(topic2, message -> {});

        channelManager.unsubscribe(topic1);
        verify(mockChannelMonitorAgent).consumerTopicRemoved(topic1);
        verify(mockChannelMonitorAgent, never()).consumerTopicRemoved(topic2);
    }

}