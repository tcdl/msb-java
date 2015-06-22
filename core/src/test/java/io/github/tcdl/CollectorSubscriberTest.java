package io.github.tcdl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.messages.Message;
import io.github.tcdl.monitor.ChannelMonitorAgent;
import io.github.tcdl.support.JsonValidator;
import io.github.tcdl.support.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Created by ruslan on 23.06.15.
 */
@RunWith(MockitoJUnitRunner.class)
public class CollectorSubscriberTest {

    private static final String TOPIC = "collector-subscriber";

    @Mock
    private ChannelManager channelManagerMock;

    @Mock
    private Collector collectorMock;

    @Before
    public void setUp() {
        when(collectorMock.getRequestMessage()).thenReturn(TestUtils.createMsbResponseMessage(TOPIC));
    }

    @Test
    public void testHandleMessageRegisteredCollectorForTopic() {
        Message originalAndReceivedMessage = TestUtils.createMsbResponseMessage(TOPIC);
        when(collectorMock.getRequestMessage()).thenReturn(originalAndReceivedMessage);
        CollectorSubscriber collectorSubscriber = new CollectorSubscriber(channelManagerMock);
        collectorSubscriber.registerCollector(TOPIC, collectorMock);
        collectorSubscriber.handleMessage(originalAndReceivedMessage);

        verify(collectorMock).handleMessage(originalAndReceivedMessage);
    }

    @Test
    public void testHandleMessageRegisteredCollectorForTopicUnexpectedCorrelationId() {
        Message receivedMessage = TestUtils.createMsbResponseMessage(TOPIC);
        CollectorSubscriber collectorSubscriber = new CollectorSubscriber(channelManagerMock);
        collectorSubscriber.registerCollector(TOPIC, collectorMock);
        collectorSubscriber.handleMessage(receivedMessage);

        verify(collectorMock, never()).handleMessage(receivedMessage);
    }

    @Test
    public void testHandleMessageUnregisteredProperCollectorForTopic() {
        String topic = "test-handlemessage-collector-not";
        Message receivedMessage = TestUtils.createMsbResponseMessage(topic);

        CollectorSubscriber collectorSubscriber = new CollectorSubscriber(channelManagerMock);
        collectorSubscriber.registerCollector("some-other-topic", collectorMock);
        collectorSubscriber.handleMessage(receivedMessage);

        verify(collectorMock, never()).handleMessage(receivedMessage);
    }

    @Test
    public void testRegisterCollector() {
        Collector secondCollectorMock = mock(Collector.class);
        when(secondCollectorMock.getRequestMessage()).thenReturn(TestUtils.createMsbResponseMessage(TOPIC));

        CollectorSubscriber collectorSubscriber = new CollectorSubscriber(channelManagerMock);
        collectorSubscriber.registerCollector(TOPIC, collectorMock);
        collectorSubscriber.registerCollector(TOPIC, secondCollectorMock);

        assertEquals(2, collectorSubscriber.collectorsByTopic.get(TOPIC).get());
        assertEquals(2, collectorSubscriber.collectorsByCorrelationId.size());
    }

    @Test
    public void testUnsubscribeMoreCollectorsExist() {
        Collector secondCollectorMock = mock(Collector.class);
        when(secondCollectorMock.getRequestMessage()).thenReturn(TestUtils.createMsbResponseMessage(TOPIC));

        CollectorSubscriber collectorSubscriber = new CollectorSubscriber(channelManagerMock);
        collectorSubscriber.registerCollector(TOPIC, collectorMock);
        collectorSubscriber.registerCollector(TOPIC, secondCollectorMock);

        collectorSubscriber.unsubscribe(TOPIC, collectorMock);

        verify(channelManagerMock, never()).unsubscribe(TOPIC);
    }

    @Test
    public void testUnsubscribeLastCollector() {
        Collector secondCollectorMock = mock(Collector.class);
        when(secondCollectorMock.getRequestMessage()).thenReturn(TestUtils.createMsbResponseMessage(TOPIC));

        CollectorSubscriber collectorSubscriber = new CollectorSubscriber(channelManagerMock);
        collectorSubscriber.registerCollector(TOPIC, collectorMock);
        collectorSubscriber.registerCollector(TOPIC, secondCollectorMock);

        collectorSubscriber.unsubscribe(TOPIC, collectorMock);
        collectorSubscriber.unsubscribe(TOPIC, secondCollectorMock);

        verify(channelManagerMock).unsubscribe(TOPIC);
    }

}
