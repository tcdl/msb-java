package io.github.tcdl.msb.collector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.support.TestUtils;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;

@RunWith(MockitoJUnitRunner.class)
public class CollectorManagerTest {

    private static final String TOPIC = "collector-subscriber";

    @Mock
    private ChannelManager channelManagerMock;

    @Mock
    private Collector collectorMock;

    @Before
    public void setUp() {
        when(collectorMock.getRequestMessage()).thenReturn(TestUtils.createSimpleRequestMessage(TOPIC));
    }

    @Test
    public void testHandleMessageRegisteredCollectorForTopic() {
        Message originalAndReceivedMessage = TestUtils.createSimpleRequestMessage(TOPIC);
        when(collectorMock.getRequestMessage()).thenReturn(originalAndReceivedMessage);
        CollectorManager collectorManager = new CollectorManager(TOPIC, channelManagerMock);
        collectorManager.registerCollector(collectorMock);
        Optional<MessageHandler> resolved = collectorManager.resolveMessageHandler(originalAndReceivedMessage);

        assertEquals(resolved.get(), collectorMock);
    }

    @Test
    public void testHandleMessageRegisteredCollectorForTopicUnexpectedCorrelationId() {
        Message receivedMessage = TestUtils.createSimpleRequestMessage(TOPIC);
        CollectorManager collectorManager = new CollectorManager(TOPIC, channelManagerMock);
        collectorManager.registerCollector(collectorMock);
        Optional<MessageHandler> resolved = collectorManager.resolveMessageHandler(receivedMessage);

        assertFalse(resolved.isPresent());
    }

    @Test
    public void testHandleMessageUnregisteredProperCollectorForTopic() {
        String topic = "test-handle-message-collector-not";
        Message receivedMessage = TestUtils.createSimpleResponseMessage(topic);

        CollectorManager collectorManager = new CollectorManager("some-other-topic", channelManagerMock);
        collectorManager.registerCollector(collectorMock);
        Optional<MessageHandler> resolved = collectorManager.resolveMessageHandler(receivedMessage);

        assertFalse(resolved.isPresent());
    }

    @Test
    public void testRegisterCollector() {
        CollectorManager collectorManager = new CollectorManager(TOPIC, channelManagerMock);
        collectorManager.registerCollector(collectorMock);

        assertEquals(1, collectorManager.collectorsByCorrelationId.size());
        verify(channelManagerMock, times(1)).subscribeForResponses(TOPIC, collectorManager);
    }

    @Test
    public void testRegisterMultipleCollectors() {
        Collector secondCollectorMock = mock(Collector.class);
        when(secondCollectorMock.getRequestMessage()).thenReturn(TestUtils.createSimpleRequestMessage(TOPIC));

        CollectorManager collectorManager = new CollectorManager(TOPIC, channelManagerMock);
        collectorManager.registerCollector(collectorMock);
        collectorManager.registerCollector(secondCollectorMock);

        assertEquals(2, collectorManager.collectorsByCorrelationId.size());
        verify(channelManagerMock, times(1)).subscribeForResponses(TOPIC, collectorManager);
    }

    @Test
    public void testRegisterTheSameCollectorMultiplyTimes() {
        CollectorManager collectorManager = new CollectorManager(TOPIC, channelManagerMock);
        collectorManager.registerCollector(collectorMock);

        verify(channelManagerMock, times(1)).subscribeForResponses(TOPIC, collectorManager);

        reset(channelManagerMock);
        collectorManager.registerCollector(collectorMock);
    }

    @Test
    public void testUnregisterMoreCollectorsExist() {
        Collector secondCollectorMock = mock(Collector.class);
        when(secondCollectorMock.getRequestMessage()).thenReturn(TestUtils.createSimpleRequestMessage(TOPIC));

        CollectorManager collectorManager = new CollectorManager(TOPIC, channelManagerMock);
        collectorManager.registerCollector(collectorMock);
        collectorManager.registerCollector(secondCollectorMock);

        collectorManager.unregisterCollector(collectorMock);
        verify(channelManagerMock, never()).unsubscribe(TOPIC);

        collectorManager.unregisterCollector(secondCollectorMock);
        verify(channelManagerMock, never()).unsubscribe(TOPIC);
    }

    @Test
    public void testUnregisterLastCollector() {
        Collector secondCollectorMock = mock(Collector.class);
        when(secondCollectorMock.getRequestMessage()).thenReturn(TestUtils.createSimpleRequestMessage(TOPIC));

        CollectorManager collectorManager = new CollectorManager(TOPIC, channelManagerMock);
        collectorManager.registerCollector(collectorMock);
        collectorManager.registerCollector(secondCollectorMock);

        collectorManager.unregisterCollector(collectorMock);
        collectorManager.unregisterCollector(secondCollectorMock);

        verify(channelManagerMock, never()).unsubscribe(TOPIC);
    }

    @Test
    public void testUnregisterTheSameCollectorsMultiplyTimes() {
        CollectorManager collectorManager = new CollectorManager(TOPIC, channelManagerMock);
        collectorManager.registerCollector(collectorMock);

        collectorManager.unregisterCollector(collectorMock);
        collectorManager.unregisterCollector(collectorMock);
        verify(channelManagerMock, never()).unsubscribe(TOPIC);
    }

    @Test
    public void testRegisterCollectorAfterUnregisterLast() {
        CollectorManager collectorManager = new CollectorManager(TOPIC, channelManagerMock);
        collectorManager.registerCollector(collectorMock);

        collectorManager.unregisterCollector(collectorMock);
        verify(channelManagerMock, never()).unsubscribe(TOPIC);

        reset(channelManagerMock);
        collectorManager.registerCollector(collectorMock);
        verify(channelManagerMock, never()).subscribeForResponses(TOPIC, collectorManager);
    }

}
