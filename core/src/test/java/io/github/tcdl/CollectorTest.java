package io.github.tcdl;

import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.EventHandlers;
import io.github.tcdl.exception.JsonConversionException;
import io.github.tcdl.messages.Acknowledge;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;
import io.github.tcdl.support.Utils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by rdro on 4/27/2015.
 */
@RunWith(MockitoJUnitRunner.class)
public class CollectorTest {

    private static final String TOPIC = "test:collector";
    private static Message originalMessageWithPayload = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);
    private static Message originalMessageWithAck = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(TOPIC);

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

    @Mock
    private TimerProvider timerProviderMock;

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
        onMessageCaptor.getValue().call(originalMessageWithPayload);

        verify(onResponse).call(originalMessageWithPayload.getPayload());
        assertTrue(collector.getPayloadMessages().contains(originalMessageWithPayload));
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
        onMessageCaptor.getValue().call(originalMessageWithPayload);

        verify(filterMock).test(eq(originalMessageWithPayload));
        verify(onResponse).call(originalMessageWithPayload.getPayload());
        assertTrue(collector.getPayloadMessages().contains(originalMessageWithPayload));
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
        onMessageCaptor.getValue().call(originalMessageWithPayload);

        verify(filterMock).test(eq(originalMessageWithPayload));
        verify(onResponse, never()).call(originalMessageWithPayload.getPayload());
        assertFalse(collector.getPayloadMessages().contains(originalMessageWithPayload));
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
        onMessageCaptor.getValue().call(originalMessageWithAck);

        verify(filterMock).test(eq(originalMessageWithAck));
        verify(onAck).call(originalMessageWithAck.getAck());
        assertTrue(collector.getAckMessages().contains(originalMessageWithAck));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesHandleError() throws InterruptedException {
        Callback<Exception> onError = mock(Callback.class);
        when(eventHandlers.onError()).thenReturn(onError);
        JsonConversionException error = new JsonConversionException("Json invalid");
        Predicate<Message> filterMock = mockFilterResult(true);
        Collector collector = new Collector(messageOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, filterMock);

        ArgumentCaptor<Callback> onErrorCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(consumerMock).subscribe(any(), onErrorCaptor.capture());
        onErrorCaptor.getValue().call(error);

        verify(onError).call(error);
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesEndEventNoResponsesRemaining() throws InterruptedException {
        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Predicate<Message> filterMock = mockFilterResult(true);
        Collector collector = new Collector(messageOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, filterMock);

        ArgumentCaptor<Callback> onMessageCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(consumerMock).subscribe(onMessageCaptor.capture(), any());
        onMessageCaptor.getValue().call(originalMessageWithAck);

        verify(onEnd).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesNoWaitForNextResponse() throws InterruptedException {
        /*ackTimeout = 0, responseTimeout=200; waitForResponses = 1
        */
        int responseTimeout = 200;
        when(messageOptionsMock.getAckTimeout()).thenReturn(0);
        when(messageOptionsMock.getResponseTimeout()).thenReturn(responseTimeout);
        when(messageOptionsMock.getWaitForResponses()).thenReturn(1);

        Callback<Payload> onResponse = mock(Callback.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = initCollectorWithTimer(messageOptionsMock);
        collector.listenForResponses(TOPIC, null);

        ArgumentCaptor<Callback> onMessageCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(consumerMock).subscribe(onMessageCaptor.capture(), any());
        onMessageCaptor.getValue().call(originalMessageWithPayload);

        verify(onResponse).call(originalMessageWithPayload.getPayload());
        verify(timerProviderMock, never()).enableResponseTimeout(anyInt());
        verify(timerProviderMock, never()).enableAckTimeout(anyInt());
        verify(onEnd).call(anyList());
    }


    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesWaitForOneMoreResponse() throws InterruptedException {
        /*ackTimeout = 0, responseTimeout=200; waitForResponses = 2
        */
        int responseTimeout = 200;
        when(messageOptionsMock.getAckTimeout()).thenReturn(0);
        when(messageOptionsMock.getResponseTimeout()).thenReturn(responseTimeout);
        when(messageOptionsMock.getWaitForResponses()).thenReturn(2);

        Callback<Payload> onResponse = mock(Callback.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = initCollectorWithTimer(messageOptionsMock);
        collector.listenForResponses(TOPIC, null);
        ArgumentCaptor<Callback> onMessageCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(consumerMock).subscribe(onMessageCaptor.capture(), any());

        //send first response
        onMessageCaptor.getValue().call(originalMessageWithPayload);
        verify(onResponse).call(originalMessageWithPayload.getPayload());
        verify(timerProviderMock).enableResponseTimeout(anyInt());
        verify(timerProviderMock, never()).enableAckTimeout(anyInt());
        verify(onEnd, never()).call(anyList());

        //send last response
        onMessageCaptor.getValue().call(originalMessageWithPayload);
        verify(onResponse, times(2)).call(originalMessageWithPayload.getPayload());
        verify(timerProviderMock).enableResponseTimeout(anyInt());
        verify(timerProviderMock, never()).enableAckTimeout(anyInt());
        verify(onEnd).call(anyList());

    }

    @Test
    @Ignore
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesRecievePayloadButAwaitAck() throws InterruptedException {
        /*ackTimeout = 500, responseTimeout=0; waitForResponses = 1
        */
        int ackTimeoutMs = 500;
        CountDownLatch awaitEnd = new CountDownLatch(1);
        when(messageOptionsMock.getAckTimeout()).thenReturn(ackTimeoutMs);
        when(messageOptionsMock.getResponseTimeout()).thenReturn(0);
        when(messageOptionsMock.getWaitForResponses()).thenReturn(1);

        Callback<Payload> onResponse = mock(Callback.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);

        Callback<List<Message>> onEnd = mock(Callback.class);
        doAnswer(invocationOnMock -> {awaitEnd.countDown(); return onEnd;}).when(onEnd).call(anyList());
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Callback<Acknowledge> onAck = mock(Callback.class);
        when(eventHandlers.onAcknowledge()).thenReturn(onAck);

        Collector collector = new Collector(messageOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, null);

        ArgumentCaptor<Callback> onMessageCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(consumerMock).subscribe(onMessageCaptor.capture(), any());

        //send response
        onMessageCaptor.getValue().call(originalMessageWithPayload);
        verify(onResponse).call(originalMessageWithPayload.getPayload());
        verify(onAck, never()).call(any());
        verify(onEnd, never()).call(anyList());

        //send ack
        onMessageCaptor.getValue().call(originalMessageWithAck);
        verify(onAck).call(any());
        assertTrue(awaitEnd.await(1000 +200, TimeUnit.MILLISECONDS));
        verify(onEnd).call(anyList());
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

        Collector collector = initCollectorWithTimer(messageOptionsMock);
        collector.listenForResponses(TOPIC, null);
        CountDownLatch endEventFired = awaitReceiveEnd();

        ArgumentCaptor<Callback> onMessageCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(consumerMock).subscribe(onMessageCaptor.capture(), any());
        onMessageCaptor.getValue().call(originalMessageWithPayload);

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


    private Collector initCollectorWithTimer(MsbMessageOptions messageOptions) {

        Collector collector = spy(new Collector(messageOptions, msbContext, eventHandlers));
        doReturn(timerProviderMock).when(collector).initTimer();

        return collector;
    }

}
