package io.github.tcdl.monitor;

import io.github.tcdl.ChannelManager;
import io.github.tcdl.Consumer;
import io.github.tcdl.MsbContext;
import io.github.tcdl.Producer;
import io.github.tcdl.TimeoutManager;
import io.github.tcdl.config.ServiceDetails;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.MessageFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static io.github.tcdl.support.Utils.TOPIC_ANNOUNCE;
import static io.github.tcdl.support.Utils.TOPIC_HEARTBEAT;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class DefaultChannelMonitorAgentTest {
    private static final Instant CLOCK_INSTANT = Instant.parse("2007-12-03T10:15:30.00Z");

    private DefaultChannelMonitorAgent channelMonitorAgent;
    private ChannelManager mockChannelManager;

    @Before
    public void setUp() {
        mockChannelManager = mock(ChannelManager.class);
        Clock clock = Clock.fixed(CLOCK_INSTANT, ZoneId.systemDefault());
        ServiceDetails serviceDetails = new ServiceDetails.ServiceDetailsBuilder().build();
        MessageFactory messageFactory = new MessageFactory(serviceDetails, clock);
        TimeoutManager mockTimeoutManager = mock(TimeoutManager.class);
        channelMonitorAgent = new DefaultChannelMonitorAgent(new MsbContext(null, messageFactory, mockChannelManager, clock, mockTimeoutManager));
    }

    @Test
    public void testAnnounceProducerForServiceTopic() {
        channelMonitorAgent.producerTopicCreated(TOPIC_ANNOUNCE);

        verify(mockChannelManager, never()).findOrCreateProducer(anyString());
    }

    @Test
    public void testAnnounceProducerForNormalTopic() {
        String topicName = "search:parsers:facets:v1";
        Producer mockProducer = mock(Producer.class);
        when(mockChannelManager.findOrCreateProducer(TOPIC_ANNOUNCE)).thenReturn(mockProducer);

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
        verify(mockChannelManager, never()).subscribe(anyString(), Mockito.any(Consumer.Subscriber.class));
    }

    @Test
    public void testAnnounceConsumerForNormalTopic() {
        String topicName = "search:parsers:facets:v1";
        Producer mockProducer = mock(Producer.class);
        when(mockChannelManager.findOrCreateProducer(TOPIC_ANNOUNCE)).thenReturn(mockProducer);

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
        when(mockChannelManager.findOrCreateProducer(TOPIC_ANNOUNCE)).thenReturn(mockProducer);
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
        verify(mockChannelManager).subscribe(Mockito.eq(TOPIC_HEARTBEAT), Mockito.any(Consumer.Subscriber.class));
    }

    private Message verifyProducerInvokedAndReturnMessage(Producer mockProducer) {
        verify(mockChannelManager).findOrCreateProducer(TOPIC_ANNOUNCE);
        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(mockProducer).publish(messageCaptor.capture());
        return messageCaptor.getValue();
    }

    private void verifyMessageContainsTopic(Message message, String topicName) {
        assertNotNull(message.getPayload());
        assertNotNull(message.getPayload().getBody());
        assertTrue(message.getPayload().getBody().containsKey(topicName));
    }
}