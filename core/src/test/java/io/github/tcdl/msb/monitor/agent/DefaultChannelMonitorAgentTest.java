package io.github.tcdl.msb.monitor.agent;

import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.Producer;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.collector.TimeoutManager;
import io.github.tcdl.msb.config.ServiceDetails;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.message.MessageFactory;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static io.github.tcdl.msb.support.Utils.TOPIC_ANNOUNCE;
import static io.github.tcdl.msb.support.Utils.TOPIC_HEARTBEAT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultChannelMonitorAgentTest {
    private static final Instant CLOCK_INSTANT = Instant.parse("2007-12-03T10:15:30.00Z");

    private DefaultChannelMonitorAgent channelMonitorAgent;
    private ChannelManager mockChannelManager;

    @Before
    public void setUp() {
        mockChannelManager = mock(ChannelManager.class);
        Clock clock = Clock.fixed(CLOCK_INSTANT, ZoneId.systemDefault());
        ServiceDetails serviceDetails = new ServiceDetails.Builder().build();
        MessageFactory messageFactory = new MessageFactory(serviceDetails, clock, TestUtils.createMessageMapper());
        TimeoutManager mockTimeoutManager = mock(TimeoutManager.class);
        MsbContextImpl msbContext = TestUtils.createMsbContextBuilder()
                .withMessageFactory(messageFactory)
                .withChannelManager(mockChannelManager)
                .withClock(clock)
                .withTimeoutManager(mockTimeoutManager)
                .build();
        channelMonitorAgent = new DefaultChannelMonitorAgent(msbContext);
    }

    @Test
    public void testAnnounceProducerForServiceTopic() {
        channelMonitorAgent.producerTopicCreated(TOPIC_ANNOUNCE);

        verify(mockChannelManager, never()).findOrCreateProducer(anyString(), any(RequestOptions.class));
    }

    @Test
    public void testAnnounceProducerForNormalTopic() {
        String topicName = "search:parsers:facets:v1";
        Producer mockProducer = mock(Producer.class);
        when(mockChannelManager.findOrCreateProducer(eq(TOPIC_ANNOUNCE), any(RequestOptions.class))).thenReturn(mockProducer);

        // method under test
        channelMonitorAgent.producerTopicCreated(topicName);

        // verify internal data structures
        assertTrue(channelMonitorAgent.topicInfoMap.containsKey(topicName));
        assertTrue(channelMonitorAgent.topicInfoMap.get(topicName).isProducers());

        Message message = verifyProducerInvokedAndReturnMessage(mockProducer);
        verifyMessageContainsTopic(message, topicName);
    }

    @Test
    public void testAnnounceConsumerForServiceTopic() {
        channelMonitorAgent.consumerTopicCreated(TOPIC_ANNOUNCE);
        verify(mockChannelManager, never()).subscribe(anyString(), any(MessageHandler.class));
    }

    @Test
    public void testAnnounceConsumerForNormalTopic() {
        String topicName = "search:parsers:facets:v1";
        Producer mockProducer = mock(Producer.class);
        when(mockChannelManager.findOrCreateProducer(eq(TOPIC_ANNOUNCE), any(RequestOptions.class))).thenReturn(mockProducer);

        // method under test
        channelMonitorAgent.consumerTopicCreated(topicName);

        // verify internal data structures
        assertTrue(channelMonitorAgent.topicInfoMap.containsKey(topicName));
        assertTrue(channelMonitorAgent.topicInfoMap.get(topicName).isConsumers());

        Message message = verifyProducerInvokedAndReturnMessage(mockProducer);
        verifyMessageContainsTopic(message, topicName);
    }

    @Test
    public void testRemoveConsumerForNormalTopic() {
        String topicName = "search:parsers:facets:v1";
        Producer mockProducer = mock(Producer.class);
        when(mockChannelManager.findOrCreateProducer(eq(TOPIC_ANNOUNCE), any(RequestOptions.class))).thenReturn(mockProducer);
        channelMonitorAgent.consumerTopicCreated(topicName); // subscribe to the topic as a preparation step

        // method under test
        channelMonitorAgent.consumerTopicRemoved(topicName);

        assertTrue(channelMonitorAgent.topicInfoMap.containsKey(topicName));
        assertFalse(channelMonitorAgent.topicInfoMap.get(topicName).isConsumers());
    }

    @Test
    public void testMessageProduce() {
        String topicName = "search:parsers:facets:v1";

        channelMonitorAgent.producerMessageSent(topicName);

        assertTrue(channelMonitorAgent.topicInfoMap.containsKey(topicName));
        assertEquals(CLOCK_INSTANT, channelMonitorAgent.topicInfoMap.get(topicName).getLastProducedAt());
    }

    @Test
    public void testMessageConsumed() {
        String topicName = "search:parsers:facets:v1";

        channelMonitorAgent.consumerMessageReceived(topicName);

        assertTrue(channelMonitorAgent.topicInfoMap.containsKey(topicName));
        assertEquals(CLOCK_INSTANT, channelMonitorAgent.topicInfoMap.get(topicName).getLastConsumedAt());
    }

    @Test
    public void testStart() {
        ChannelMonitorAgent startedAgent = channelMonitorAgent.start();

        assertSame(channelMonitorAgent, startedAgent);
        verify(mockChannelManager).subscribe(eq(TOPIC_HEARTBEAT), any(MessageHandler.class));
    }

    private Message verifyProducerInvokedAndReturnMessage(Producer mockProducer) {
        verify(mockChannelManager).findOrCreateProducer(eq(TOPIC_ANNOUNCE), any(RequestOptions.class));
        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(mockProducer).publish(messageCaptor.capture());
        return messageCaptor.getValue();
    }

    private void verifyMessageContainsTopic(Message message, String topicName) {
        assertNotNull(message.getRawPayload());
        assertNotNull(message.getRawPayload().get("body"));
        assertTrue(message.getRawPayload().get("body").has(topicName));
    }
}