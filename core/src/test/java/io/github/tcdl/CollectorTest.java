package io.github.tcdl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.EventHandlers;
import io.github.tcdl.messages.Acknowledge;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;
import io.github.tcdl.support.Utils;

import java.io.IOException;
import java.time.Clock;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Created by rdro on 4/27/2015.
 */
@RunWith(MockitoJUnitRunner.class)
public class CollectorTest {

    private static final String TOPIC = "test:collector";
    private static Message originaMessageWithPayload = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);
    private static Message originaMessageWithAck = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(TOPIC);

    @Mock
    private MessageFactory messageFactoryMock;

    @Mock
    private MsbMessageOptions messageOptionsMock;

    @Mock
    private ChannelManager channelManagerMock;

    @Mock
    private Consumer consumerMock;

    @Mock
    private EventHandlers eventHandlers;

    @Mock
    private MsbConfigurations msbConfigurationsMock;

    private MsbContext msbContext;

    @Before
    public void setUp() throws IOException {
        when(channelManagerMock.findOrCreateConsumer(anyString())).thenReturn(consumerMock);

        msbContext = new MsbContext(msbConfigurationsMock, messageFactoryMock, channelManagerMock, Clock.systemDefaultZone());
    }

    @Test(expected = NullPointerException.class)
    public void testCreateCollectorNullMsgOptions() {
        new Collector(null, msbContext, eventHandlers);
    }

    @Test
    public void testGetWaitForResponsesConfigsNoWaitForResponsesReturnFalse() {
        when(messageOptionsMock.getWaitForResponses()).thenReturn(null);
        Collector collector = new Collector(messageOptionsMock, msbContext, eventHandlers);
        assertFalse("expect false if MessageOptions.waitForResponses null", collector.isWaitForResponses());
    }

    @Test
    public void testGetWaitForResponsesConfigsReturnFalse() {
        when(messageOptionsMock.getWaitForResponses()).thenReturn(-1);
        Collector collector = new Collector(messageOptionsMock, msbContext, eventHandlers);
        assertFalse("expect false if MessageOptions.waitForResponses equals -1", collector.isWaitForResponses());
    }

    @Test
    public void testGetWaitForResponsesConfigsReturnTrue() {
        when(messageOptionsMock.getWaitForResponses()).thenReturn(100);
        Collector collector = new Collector(messageOptionsMock, msbContext, eventHandlers);
        assertTrue("expect true if MessageOptions.waitForResponses equals 100", collector.isWaitForResponses());
    }

    @Test
    public void testIsAwaitingAcksConfigsNotSetAckTimeoutReturnFalse() {
        when(messageOptionsMock.getAckTimeout()).thenReturn(null);
        Collector collector = new Collector(messageOptionsMock, msbContext, eventHandlers);
        assertFalse("expect false if MessageOptions.ackTimeout null", collector.isAwaitingAcks());
    }

    @Test
    public void testIsAwaitingAcksReturnTrue() {
        when(messageOptionsMock.getAckTimeout()).thenReturn(200);
        Collector collector = new Collector(messageOptionsMock, msbContext, eventHandlers);
        assertTrue("expect true if MessageOptions.ackTimeout equals 200", collector.isAwaitingAcks());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesFilterNull() throws InterruptedException {
        Callback<Payload> onResponse = mock(Callback.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        Collector collector = new Collector(messageOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, null);

        ArgumentCaptor<Callback> onMessageCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(consumerMock).subscribe(onMessageCaptor.capture(), any());
        onMessageCaptor.getValue().call(originaMessageWithPayload);

        verify(onResponse).call(originaMessageWithPayload.getPayload());
        assertTrue(collector.getPayloadMessages().contains(originaMessageWithPayload));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesFilterReturnTrue() throws InterruptedException {
        Predicate<Message> filterMock = mockFilterResult(true);
        Callback<Payload> onResponse = mock(Callback.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        Collector collector = new Collector(messageOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, filterMock);

        ArgumentCaptor<Callback> onMessageCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(consumerMock).subscribe(onMessageCaptor.capture(), any());
        onMessageCaptor.getValue().call(originaMessageWithPayload);

        verify(filterMock).test(eq(originaMessageWithPayload));
        verify(onResponse).call(originaMessageWithPayload.getPayload());
        assertTrue(collector.getPayloadMessages().contains(originaMessageWithPayload));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesFilterReturnFalse() throws InterruptedException {
        Callback<Payload> onResponse = mock(Callback.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        Predicate<Message> filterMock = mockFilterResult(false);
        Collector collector = new Collector(messageOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, filterMock);

        ArgumentCaptor<Callback> onMessageCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(consumerMock).subscribe(onMessageCaptor.capture(), any());
        onMessageCaptor.getValue().call(originaMessageWithPayload);

        verify(filterMock).test(eq(originaMessageWithPayload));
        verify(onResponse, never()).call(originaMessageWithPayload.getPayload());
        assertFalse(collector.getPayloadMessages().contains(originaMessageWithPayload));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesFilterReturnTrueRecievedAck() throws InterruptedException {
        Callback<Acknowledge> onAck = mock(Callback.class);
        when(eventHandlers.onAcknowledge()).thenReturn(onAck);
        Predicate<Message> filterMock = mockFilterResult(true);
        Collector collector = new Collector(messageOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, filterMock);

        ArgumentCaptor<Callback> onMessageCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(consumerMock).subscribe(onMessageCaptor.capture(), any());
        onMessageCaptor.getValue().call(originaMessageWithAck);

        verify(filterMock).test(eq(originaMessageWithAck));
        verify(onAck).call(originaMessageWithAck.getAck());
        assertTrue(collector.getAckMessages().contains(originaMessageWithAck));
    }


    @Test
    @Ignore
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesWaitForOneMoreResponseNoTimeout() throws InterruptedException {
        /*ackTimeout = 200, responseTimeout=200; waitForResponses = 2
        */
        int ackAndTimeoutMs = 200;
        when(messageOptionsMock.getAckTimeout()).thenReturn(ackAndTimeoutMs);
        when(messageOptionsMock.getResponseTimeout()).thenReturn(ackAndTimeoutMs);
        when(messageOptionsMock.getWaitForResponses()).thenReturn(2);

        Collector collector = new Collector(messageOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, null);
        CountDownLatch endEventFired = awaitReceiveEnd();

        ArgumentCaptor<Callback> onMessageCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(consumerMock).subscribe(onMessageCaptor.capture(), any());
        onMessageCaptor.getValue().call(originaMessageWithPayload);

        //give extra time for processing scheduled end() task after ack timeout
        assertFalse(endEventFired.await(ackAndTimeoutMs + 100, TimeUnit.MILLISECONDS));
    }

    @Test
    @Ignore
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesRecievePayloadAwaitAck() throws InterruptedException {
        /*ackTimeout = 200, responseTimeout=200; waitForResponses = 1
        */
        int ackimeoutMs = 200;
        when(messageOptionsMock.getAckTimeout()).thenReturn(ackimeoutMs);
        when(messageOptionsMock.getWaitForResponses()).thenReturn(1);

        Collector collector = new Collector(messageOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, null);
        CountDownLatch endEventFired = awaitReceiveEnd();

        ArgumentCaptor<Callback> onMessageCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(consumerMock).subscribe(onMessageCaptor.capture(), any());
        onMessageCaptor.getValue().call(originaMessageWithPayload);

        //give extra time for processing scheduled end() task after ack timeout
        assertTrue(endEventFired.await(ackimeoutMs + 100, TimeUnit.MILLISECONDS));
    }

    @Test
    @Ignore
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponseTimeoutAckTaskMoreReponsesNoTimeout() throws InterruptedException {
        /*ackTimeout = 200, responseTimeout=200; waitForResponses = 1
        */
        int ackimeoutMs = 200;
        when(messageOptionsMock.getAckTimeout()).thenReturn(ackimeoutMs);
        when(messageOptionsMock.getWaitForResponses()).thenReturn(1);
        Acknowledge ack = new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId()).setResponsesRemaining(1).build();
        Message messageWithAck = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(
                ack, TOPIC);

        Collector collector = new Collector(messageOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, null);
        CountDownLatch endEventFired = awaitReceiveEnd();

        ArgumentCaptor<Callback> onMessageCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(consumerMock).subscribe(onMessageCaptor.capture(), any());
        onMessageCaptor.getValue().call(originaMessageWithPayload);

        //send ack message with responsesRemaining between ackTimeout task scheduled and run 
        onMessageCaptor.getValue().call(messageWithAck);

        //give extra time for processing scheduled end() task after ack timeout
        assertFalse(endEventFired.await(ackimeoutMs + 100, TimeUnit.MILLISECONDS));
    }

    @Test
    @Ignore
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesRecievedAckWithTimeoutRunAckTimeoutTask() throws InterruptedException {
        //verify that ackTimeout will fire END event if no more responses remaining 
        int timeoutMs = 3000;
        int ackTimeoutMs = 250;
        when(messageOptionsMock.getAckTimeout()).thenReturn(ackTimeoutMs);
        when(messageOptionsMock.getResponseTimeout()).thenReturn(3000);
        Acknowledge ack = new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId()).setResponsesRemaining(0).setTimeoutMs(timeoutMs).build();
        Message messageWithAck = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(
                ack, TOPIC);
        Collector collector = new Collector(messageOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, null);
        CountDownLatch endEventFired = awaitReceiveEnd();

        ArgumentCaptor<Callback> onMessageCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(consumerMock).subscribe(onMessageCaptor.capture(), any());
        onMessageCaptor.getValue().call(messageWithAck);

        //give extra time for processing scheduled end() task
        assertTrue(endEventFired.await(ackTimeoutMs + 100, TimeUnit.MILLISECONDS));
        verify(channelManagerMock).removeConsumer(TOPIC);
    }

    @Test
    @Ignore
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesRecievedAckWithTimeoutRunTimeoutTask() throws InterruptedException {
        //verify that timeout will fire END event if  more responses remaining 
        int timeoutMs = 250;
        int ackTimeoutMs = 3000;
        when(messageOptionsMock.getAckTimeout()).thenReturn(ackTimeoutMs);
        when(messageOptionsMock.getResponseTimeout()).thenReturn(0);
        Acknowledge ack = new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId()).setResponsesRemaining(1).setTimeoutMs(timeoutMs).build();
        Message messageWithAck = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(
                ack, TOPIC);
        Collector collector = new Collector(messageOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, null);
        CountDownLatch endEventFired = awaitReceiveEnd();

        ArgumentCaptor<Callback> onMessageCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(consumerMock).subscribe(onMessageCaptor.capture(), any());
        onMessageCaptor.getValue().call(messageWithAck);

        //give extra time for processing scheduled end() task
        assertTrue(endEventFired.await(timeoutMs + 100, TimeUnit.MILLISECONDS));
        verify(channelManagerMock).removeConsumer(TOPIC);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Ignore
    @Test
    public void testListenForResponsesRecievedAckWithTimeoutResponsesRemaining() throws InterruptedException {
        int timeoutMs = 300;
        int ackTimeoutMs = 5000;
        when(messageOptionsMock.getAckTimeout()).thenReturn(ackTimeoutMs);
        Collector collector = new Collector(messageOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, null);
        CountDownLatch endEventFired = awaitReceiveEnd();
        Message messageSetResponsesRemaining = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(
                new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId()).setResponsesRemaining(1).setTimeoutMs(timeoutMs).build(), TOPIC);

        ArgumentCaptor<Callback> onMessageCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(consumerMock).subscribe(onMessageCaptor.capture(), any());
        onMessageCaptor.getValue().call(messageSetResponsesRemaining);

        //give extra time for processing scheduled end() task
        assertTrue(endEventFired.await(timeoutMs + 200, TimeUnit.MILLISECONDS));
        verify(channelManagerMock).removeConsumer(TOPIC);
    }

    private CountDownLatch awaitReceiveEnd() {
        CountDownLatch eventEndOnAck = new CountDownLatch(1);
        eventHandlers.onEnd(messages -> eventEndOnAck.countDown());
        return eventEndOnAck;
    }

    @SuppressWarnings("unchecked")
    private Predicate<Message> mockFilterResult(boolean result) {
        Predicate<Message> filterMock = mock(Predicate.class);
        when(filterMock.test(any(Message.class))).thenReturn(result);
        return filterMock;
    }
}
