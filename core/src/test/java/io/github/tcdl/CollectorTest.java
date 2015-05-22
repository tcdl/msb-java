package io.github.tcdl;

import static io.github.tcdl.events.Event.MESSAGE_EVENT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.Event;
import io.github.tcdl.events.SingleArgEventHandler;
import io.github.tcdl.messages.Acknowledge;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;
import io.github.tcdl.support.Utils;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Created by rdro on 4/27/2015.
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest({ ChannelManager.class })
public class CollectorTest {

    private static final String TOPIC = "test:collector";
    private static Message originaMessageWithPayload = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);
    private static Message originaMessageWithAck = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(TOPIC);

    @Mock
    private MsbMessageOptions messageOptionsMock;

    @Mock
    private ChannelManager channelManagerMock;

    @Mock
    private MsbConfigurations msbConfigurationMock;

    @Before
    public void setUp() throws IOException {
        // Setup channel mock
        mockStatic(ChannelManager.class);

        PowerMockito.when(ChannelManager.getInstance()).thenReturn(channelManagerMock);

    }

    @Test(expected = NullPointerException.class)
    public void testCreateCollectorNullMsgOptions() {
        new Collector(null);
    }

    @Test
    public void testGetWaitForResponsesConfigsNoWaitForResponsesReturnFalse() {
        when(messageOptionsMock.getWaitForResponses()).thenReturn(null);
        Collector collector = new Collector(messageOptionsMock);
        assertFalse("expect false if MessageOptions.waitForResponses null", collector.isWaitForResponses());
    }

    @Test
    public void testGetWaitForResponsesConfigsReturnFalse() {
        when(messageOptionsMock.getWaitForResponses()).thenReturn(-1);
        Collector collector = new Collector(messageOptionsMock);
        assertFalse("expect false if MessageOptions.waitForResponses equals -1", collector.isWaitForResponses());
    }

    @Test
    public void testGetWaitForResponsesConfigsReturnTrue() {
        when(messageOptionsMock.getWaitForResponses()).thenReturn(100);
        Collector collector = new Collector(messageOptionsMock);
        assertTrue("expect true if MessageOptions.waitForResponses equals 100", collector.isWaitForResponses());
    }

    @Test
    public void testIsAwaitingAcksConfigsNotSetAckTimeoutReturnFalse() {
        when(messageOptionsMock.getAckTimeout()).thenReturn(null);
        Collector collector = new Collector(messageOptionsMock);
        assertFalse("expect false if MessageOptions.ackTimeout null", collector.isAwaitingAcks());
    }

    @Test
    public void testIsAwaitingAcksReturnTrue() {
        when(messageOptionsMock.getAckTimeout()).thenReturn(200);
        Collector collector = new Collector(messageOptionsMock);
        assertTrue("expect true if MessageOptions.ackTimeout equals 200", collector.isAwaitingAcks());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesFilterNull() throws InterruptedException {
        Collector collector = new Collector(messageOptionsMock);
        collector.listenForResponses(TOPIC, null);
        CountDownLatch eventsFired = awaitRecievePayloadEvents(collector);

        ArgumentCaptor<SingleArgEventHandler> onMessageCaptur = ArgumentCaptor.forClass(SingleArgEventHandler.class);
        verify(channelManagerMock).on(eq(MESSAGE_EVENT), onMessageCaptur.capture());
        onMessageCaptur.getValue().onEvent(originaMessageWithPayload);

        assertTrue(collector.getPayloadMessages().contains(originaMessageWithPayload));
        assertTrue(eventsFired.await(100, TimeUnit.MILLISECONDS));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesFilterReturnTrue() throws InterruptedException {
        Collector collector = new Collector(messageOptionsMock);
        Predicate<Message> filterMock = mockFilterResult(true);
        collector.listenForResponses(TOPIC, filterMock);
        CountDownLatch eventsFired = awaitRecievePayloadEvents(collector);

        ArgumentCaptor<SingleArgEventHandler> onMessageCaptur = ArgumentCaptor.forClass(SingleArgEventHandler.class);
        verify(channelManagerMock).on(eq(MESSAGE_EVENT), onMessageCaptur.capture());
        onMessageCaptur.getValue().onEvent(originaMessageWithPayload);

        verify(filterMock).test(eq(originaMessageWithPayload));
        assertTrue(collector.getPayloadMessages().contains(originaMessageWithPayload));
        assertTrue(eventsFired.await(100, TimeUnit.MILLISECONDS));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesFilterReturnFalse() throws InterruptedException {
        Collector collector = new Collector(messageOptionsMock);
        Predicate<Message> filterMock = mockFilterResult(false);
        collector.listenForResponses(TOPIC, filterMock);
        CountDownLatch eventsFired = awaitRecievePayloadEvents(collector);

        ArgumentCaptor<SingleArgEventHandler> onMessageCaptur = ArgumentCaptor.forClass(SingleArgEventHandler.class);
        verify(channelManagerMock).on(eq(MESSAGE_EVENT), onMessageCaptur.capture());
        onMessageCaptur.getValue().onEvent(originaMessageWithPayload);

        verify(filterMock).test(eq(originaMessageWithPayload));
        assertFalse(collector.getPayloadMessages().contains(originaMessageWithPayload));
        assertFalse(eventsFired.await(100, TimeUnit.MILLISECONDS));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesRecievePayloadAwaitAck() throws InterruptedException {
        int ackTimeoutMs = 250;
        when(messageOptionsMock.getAckTimeout()).thenReturn(ackTimeoutMs);
        Collector collector = new Collector(messageOptionsMock);
        collector.listenForResponses(TOPIC, null);
        CountDownLatch endEventFired = awaitRecieveEndEvent(collector);

        ArgumentCaptor<SingleArgEventHandler> onMessageCaptur = ArgumentCaptor.forClass(SingleArgEventHandler.class);
        verify(channelManagerMock).on(eq(MESSAGE_EVENT), onMessageCaptur.capture());
        onMessageCaptur.getValue().onEvent(originaMessageWithPayload);

        //give extra time for processing scheduled end() task after ack timeout
        assertTrue(endEventFired.await(ackTimeoutMs + 100, TimeUnit.MILLISECONDS));
        verify(channelManagerMock).removeConsumer(TOPIC);
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesRecievedAck() throws InterruptedException {
        Collector collector = new Collector(messageOptionsMock);
        Predicate<Message> filterMock = mockFilterResult(true);
        collector.listenForResponses(TOPIC, filterMock);
        CountDownLatch eventsFired = awaitRecieveAckEvents(collector);

        ArgumentCaptor<SingleArgEventHandler> onMessageCaptur = ArgumentCaptor.forClass(SingleArgEventHandler.class);
        verify(channelManagerMock).on(eq(MESSAGE_EVENT), onMessageCaptur.capture());
        onMessageCaptur.getValue().onEvent(originaMessageWithAck);

        verify(filterMock).test(eq(originaMessageWithAck));
        assertTrue(collector.getAckMessages().contains(originaMessageWithAck));
        assertTrue(eventsFired.await(100, TimeUnit.MILLISECONDS));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesRecievedAckWithTimeoutAwaitAckTimeoutMs() throws InterruptedException {
        int timeoutMs = 5000;
        int ackTimeoutMs = 300;
        when(messageOptionsMock.getAckTimeout()).thenReturn(ackTimeoutMs);
        Collector collector = new Collector(messageOptionsMock);
        collector.listenForResponses(TOPIC, null);
        CountDownLatch endEventFired = awaitRecieveEndEvent(collector);
        Message messageWithAck = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(
                new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId()).setTimeoutMs(timeoutMs).build(), TOPIC);

        ArgumentCaptor<SingleArgEventHandler> onMessageCaptur = ArgumentCaptor.forClass(SingleArgEventHandler.class);
        verify(channelManagerMock).on(eq(MESSAGE_EVENT), onMessageCaptur.capture());
        onMessageCaptur.getValue().onEvent(messageWithAck);

        //give extra time for processing scheduled end() task
        assertTrue(endEventFired.await(ackTimeoutMs + 100, TimeUnit.MILLISECONDS));
        verify(channelManagerMock).removeConsumer(TOPIC);
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesRecievedAckWithTimeoutAwaitTimeoutMs() throws InterruptedException {
        int timeoutMs = 300;
        int ackTimeoutMs = 5000;
        when(messageOptionsMock.getAckTimeout()).thenReturn(ackTimeoutMs);
        Collector collector = new Collector(messageOptionsMock);
        collector.listenForResponses(TOPIC, null);
        CountDownLatch endEventFired = awaitRecieveEndEvent(collector);
        Message messageWithAck = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(
                new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId()).setTimeoutMs(timeoutMs).build(), TOPIC);

        ArgumentCaptor<SingleArgEventHandler> onMessageCaptur = ArgumentCaptor.forClass(SingleArgEventHandler.class);
        verify(channelManagerMock).on(eq(MESSAGE_EVENT), onMessageCaptur.capture());
        onMessageCaptur.getValue().onEvent(messageWithAck);

        //give extra time for processing scheduled end() task
        assertTrue(endEventFired.await(timeoutMs + 100, TimeUnit.MILLISECONDS));
        verify(channelManagerMock).removeConsumer(TOPIC);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testListenForResponsesRecievedAckWithTimeoutResponsesRemaining() throws InterruptedException {
        int timeoutMs = 300;
        int ackTimeoutMs = 5000;
        when(messageOptionsMock.getAckTimeout()).thenReturn(ackTimeoutMs);
        Collector collector = new Collector(messageOptionsMock);
        collector.listenForResponses(TOPIC, null);
        CountDownLatch endEventFired = awaitRecieveEndEvent(collector);
        Message messageSetResponsesRemaining = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(
                new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId()).setResponsesRemaining(1).setTimeoutMs(timeoutMs).build(), TOPIC);

        ArgumentCaptor<SingleArgEventHandler> onMessageCaptur = ArgumentCaptor.forClass(SingleArgEventHandler.class);
        verify(channelManagerMock).on(eq(MESSAGE_EVENT), onMessageCaptur.capture());
        onMessageCaptur.getValue().onEvent(messageSetResponsesRemaining);

        //give extra time for processing scheduled end() task
        assertTrue(endEventFired.await(timeoutMs + 100, TimeUnit.MILLISECONDS));
        verify(channelManagerMock).removeConsumer(TOPIC);
    }

    private CountDownLatch awaitRecievePayloadEvents(Collector collector) {
        CountDownLatch eventsExpectedOnPayload = new CountDownLatch(2);
        collector.on(Event.PAYLOAD_EVENT, (Payload p) -> eventsExpectedOnPayload.countDown());
        collector.on(Event.RESPONSE_EVENT, (Payload p) -> eventsExpectedOnPayload.countDown());
        return eventsExpectedOnPayload;
    }

    private CountDownLatch awaitRecieveAckEvents(Collector collector) {
        CountDownLatch eventExpectedOnAck = new CountDownLatch(1);
        collector.on(Event.ACKNOWLEDGE_EVENT, (Acknowledge a) -> eventExpectedOnAck.countDown());
        return eventExpectedOnAck;
    }

    private CountDownLatch awaitRecieveEndEvent(Collector collector) {
        CountDownLatch eventEndOnAck = new CountDownLatch(1);
        collector.on(Event.END_EVENT, (Object[] a) -> eventEndOnAck.countDown());
        return eventEndOnAck;
    }

    @SuppressWarnings("unchecked")
    private Predicate<Message> mockFilterResult(boolean result) {
        Predicate<Message> filterMock = mock(Predicate.class);
        when(filterMock.test(any(Message.class))).thenReturn(result);
        return filterMock;
    }
}
