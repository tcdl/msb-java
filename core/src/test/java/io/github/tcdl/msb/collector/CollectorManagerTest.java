package io.github.tcdl.msb.collector;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CollectorManagerTest {

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
        CollectorManager collectorManager = new CollectorManager(TOPIC, channelManagerMock);
        collectorManager.registerCollector(collectorMock);
        collectorManager.handleMessage(originalAndReceivedMessage);

        verify(collectorMock).handleMessage(originalAndReceivedMessage);
    }

    @Test
    public void testHandleMessageRegisteredCollectorForTopicUnexpectedCorrelationId() {
        Message receivedMessage = TestUtils.createMsbResponseMessage(TOPIC);
        CollectorManager collectorManager = new CollectorManager(TOPIC, channelManagerMock);
        collectorManager.registerCollector(collectorMock);
        collectorManager.handleMessage(receivedMessage);

        verify(collectorMock, never()).handleMessage(receivedMessage);
    }

    @Test
    public void testHandleMessageUnregisteredProperCollectorForTopic() {
        String topic = "test-handlemessage-collector-not";
        Message receivedMessage = TestUtils.createMsbResponseMessage(topic);

        CollectorManager collectorManager = new CollectorManager("some-other-topic", channelManagerMock);
        collectorManager.registerCollector(collectorMock);
        collectorManager.handleMessage(receivedMessage);

        verify(collectorMock, never()).handleMessage(receivedMessage);
    }

    @Test
    public void testRegisterCollector() {
        Collector secondCollectorMock = mock(Collector.class);
        when(secondCollectorMock.getRequestMessage()).thenReturn(TestUtils.createMsbResponseMessage(TOPIC));

        CollectorManager collectorManager = new CollectorManager(TOPIC, channelManagerMock);
        collectorManager.registerCollector(collectorMock);
        collectorManager.registerCollector(secondCollectorMock);

        assertEquals(2, collectorManager.collectorsByCorrelationId.size());
        verify(channelManagerMock, times(2)).subscribe(TOPIC, collectorManager);
    }

    @Test
    public void testUnsubscribeMoreCollectorsExist() {
        Collector secondCollectorMock = mock(Collector.class);
        when(secondCollectorMock.getRequestMessage()).thenReturn(TestUtils.createMsbResponseMessage(TOPIC));

        CollectorManager collectorManager = new CollectorManager(TOPIC, channelManagerMock);
        collectorManager.registerCollector(collectorMock);
        collectorManager.registerCollector(secondCollectorMock);

        collectorManager.unsubscribe(collectorMock);
        verify(channelManagerMock, never()).unsubscribe(TOPIC);

        collectorManager.unsubscribe(secondCollectorMock);
        verify(channelManagerMock).unsubscribe(TOPIC);
    }

    @Test
    public void testUnsubscribeLastCollector() {
        Collector secondCollectorMock = mock(Collector.class);
        when(secondCollectorMock.getRequestMessage()).thenReturn(TestUtils.createMsbResponseMessage(TOPIC));

        CollectorManager collectorManager = new CollectorManager(TOPIC, channelManagerMock);
        collectorManager.registerCollector(collectorMock);
        collectorManager.registerCollector(secondCollectorMock);

        collectorManager.unsubscribe(collectorMock);
        collectorManager.unsubscribe(secondCollectorMock);

        verify(channelManagerMock).unsubscribe(TOPIC);
    }

}
