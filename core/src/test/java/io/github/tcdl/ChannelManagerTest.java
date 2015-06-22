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
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.EventHandlers;
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
    public void testSubscribeUnsubscribeOne() {
        String topic = "topic:test-unsubscribe-once";
        Collector collector =new Collector(mock(MsbMessageOptions.class),  TestUtils.createSimpleMsbContext(), mock(EventHandlers.class));
        Message requestMessage = TestUtils.createMsbRequestMessageNoPayload();
        collector.listenForResponses(topic, requestMessage);

        channelManager.subscribe(topic, collector);
        channelManager.unsubscribe(topic, requestMessage.getCorrelationId());

        verify(mockChannelMonitorAgent).consumerTopicRemoved(topic);
    }

    @Test
    public void testSubscribeUnsubscribeMultiple() {
        String topic = "topic:test-unsubscribe-multi";
        Collector collector1 =new Collector(mock(MsbMessageOptions.class), TestUtils.createSimpleMsbContext(), mock(EventHandlers.class));
        Message requestMessage1 = TestUtils.createMsbRequestMessageNoPayload();
        collector1.listenForResponses(topic, requestMessage1);

        Collector collector2 =new Collector(mock(MsbMessageOptions.class), TestUtils.createSimpleMsbContext(), mock(EventHandlers.class));
        Message requestMessage2 = TestUtils.createMsbRequestMessageNoPayload();
        collector2.listenForResponses(topic, requestMessage2);

        channelManager.subscribe(topic, collector1);
        channelManager.subscribe(topic, collector2);

        channelManager.unsubscribe(topic, requestMessage1.getCorrelationId());
        verify(mockChannelMonitorAgent,  never()).consumerTopicRemoved(topic);

        channelManager.unsubscribe(topic, requestMessage2.getCorrelationId());
        verify(mockChannelMonitorAgent).consumerTopicRemoved(topic);
    }

    @Test
    public void testSubscribeUnsubscribeSeparateTopics() {
        String topic1 = "topic:test-unsubscribe-try-first";
        Collector collector1 =new Collector(mock(MsbMessageOptions.class), TestUtils.createSimpleMsbContext(), mock(EventHandlers.class));
        Message requestMessage1 = TestUtils.createMsbRequestMessageNoPayload();
        collector1.listenForResponses(topic1, requestMessage1);

        String topic2 = "topic:test-unsubscribe-try-other";
        Collector collector2 =new Collector(mock(MsbMessageOptions.class), TestUtils.createSimpleMsbContext(), mock(EventHandlers.class));
        Message requestMessage2 = TestUtils.createMsbRequestMessageNoPayload();
        collector2.listenForResponses(topic2, requestMessage2);

        channelManager.subscribe(topic1, collector1);
        channelManager.subscribe(topic2, collector2);

        channelManager.unsubscribe(topic1, requestMessage1.getCorrelationId());
        verify(mockChannelMonitorAgent).consumerTopicRemoved(topic1);
    }

}