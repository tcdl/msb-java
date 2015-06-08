package io.github.tcdl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.function.Predicate;

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
    public void testListenForResponsesFilterNull() {
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
    public void testListenForResponsesFilterReturnTrue() {
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
    public void testListenForResponsesFilterReturnFalse() {
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
    public void testListenForResponsesFilterReturnTrueReceivedAck() {
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
    public void testListenForResponsesHandleError() {
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
    public void testListenForResponsesEndEventNoResponsesRemaining() {
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
    public void testListenForResponsesLastResponse() {
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
    public void testListenForResponsesWaitForOneMoreResponse() {
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
        verify(onEnd, never()).call(anyList());

        //send last response
        onMessageCaptor.getValue().call(originalMessageWithPayload);
        verify(onResponse, times(2)).call(originalMessageWithPayload.getPayload());
        verify(timerProviderMock, never()).enableResponseTimeout(anyInt());
        verify(timerProviderMock, never()).enableAckTimeout(anyInt());
        verify(onEnd).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesNoResponsesRemainingButAwaitAck() {
        /*ackTimeout = 100, responseTimeout=0; waitForResponses = 0
        */
        int ackTimeoutMs = 100;
        when(messageOptionsMock.getAckTimeout()).thenReturn(ackTimeoutMs);
        when(messageOptionsMock.getResponseTimeout()).thenReturn(0);
        when(messageOptionsMock.getWaitForResponses()).thenReturn(0);

        Callback<Acknowledge> onAck = mock(Callback.class);
        when(eventHandlers.onAcknowledge()).thenReturn(onAck);
        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = initCollectorWithTimer(messageOptionsMock);
        collector.listenForResponses(TOPIC, null);
        ArgumentCaptor<Callback> onMessageCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(consumerMock).subscribe(onMessageCaptor.capture(), any());

        //send payload response
        onMessageCaptor.getValue().call(originalMessageWithPayload);
        verify(timerProviderMock, never()).enableResponseTimeout(anyInt());
        verify(timerProviderMock).enableAckTimeout(anyInt());
        verify(onEnd, never()).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesReceivedPayloadButAwaitAck() {
        /*ackTimeout = 100, responseTimeout=0; waitForResponses = 1
        */
        int ackTimeoutMs = 100;
        when(messageOptionsMock.getAckTimeout()).thenReturn(ackTimeoutMs);
        when(messageOptionsMock.getResponseTimeout()).thenReturn(0);
        when(messageOptionsMock.getWaitForResponses()).thenReturn(1);

        Callback<Acknowledge> onAck = mock(Callback.class);
        when(eventHandlers.onAcknowledge()).thenReturn(onAck);
        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = initCollectorWithTimer(messageOptionsMock);
        collector.listenForResponses(TOPIC, null);
        ArgumentCaptor<Callback> onMessageCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(consumerMock).subscribe(onMessageCaptor.capture(), any());

        //send payload response
        onMessageCaptor.getValue().call(originalMessageWithPayload);
        verify(timerProviderMock, never()).enableResponseTimeout(anyInt());
        verify(timerProviderMock).enableAckTimeout(anyInt());
        verify(onEnd, never()).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesReceivedAckWithSameTimeoutValue() {
         /*ackTimeout = 0, responseTimeout= 50; waitForResponses = 0
        */
        int timeoutMs = 50;
        when(messageOptionsMock.getAckTimeout()).thenReturn(0);
        when(messageOptionsMock.getResponseTimeout()).thenReturn(timeoutMs);
        when(messageOptionsMock.getWaitForResponses()).thenReturn(0);

        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Acknowledge ack = new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId()).setResponsesRemaining(0).setTimeoutMs(timeoutMs).build();
        Message messageWithAck = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(
                ack, TOPIC);
        Collector collector = initCollectorWithTimer(messageOptionsMock);
        collector.listenForResponses(TOPIC, null);

        ArgumentCaptor<Callback> onMessageCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(consumerMock).subscribe(onMessageCaptor.capture(), any());
        onMessageCaptor.getValue().call(messageWithAck);

        verify(timerProviderMock, never()).enableResponseTimeout(timeoutMs);
        verify(onEnd).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesReceivedAckWithUpdatedTimeoutAndNoResponsesRemaining() {
         /*ackTimeout = 0, responseTimeout= 50; waitForResponses = 0
        */
        int timeoutMs = 50;
        int timeoutMsInAck = 100;
        when(messageOptionsMock.getAckTimeout()).thenReturn(0);
        when(messageOptionsMock.getResponseTimeout()).thenReturn(timeoutMs);
        when(messageOptionsMock.getWaitForResponses()).thenReturn(0);

        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Acknowledge ack = new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId()).setResponsesRemaining(0).setTimeoutMs(timeoutMsInAck).build();
        Message messageWithAck = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(
                ack, TOPIC);
        Collector collector = initCollectorWithTimer(messageOptionsMock);
        collector.listenForResponses(TOPIC, null);

        ArgumentCaptor<Callback> onMessageCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(consumerMock).subscribe(onMessageCaptor.capture(), any());
        onMessageCaptor.getValue().call(messageWithAck);

        verify(timerProviderMock).enableResponseTimeout(timeoutMsInAck);
        verify(onEnd).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesReceivedAckWithUpdatedTimeoutAndResponsesRemaining() {
        /*ackTimeout = 0, responseTimeout= 50; waitForResponses = 2
        */
        int timeoutMs = 50;
        int timeoutMsInAck = 100;
        int responsesRemaining = 2;
        when(messageOptionsMock.getAckTimeout()).thenReturn(0);
        when(messageOptionsMock.getResponseTimeout()).thenReturn(timeoutMs);
        when(messageOptionsMock.getWaitForResponses()).thenReturn(0);

        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Acknowledge ack = new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId()).setResponsesRemaining(responsesRemaining)
                .setTimeoutMs(timeoutMsInAck).build();
        Message messageWithAck = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(
                ack, TOPIC);
        Collector collector = initCollectorWithTimer(messageOptionsMock);
        collector.listenForResponses(TOPIC, null);

        ArgumentCaptor<Callback> onMessageCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(consumerMock).subscribe(onMessageCaptor.capture(), any());
        onMessageCaptor.getValue().call(messageWithAck);

        verify(timerProviderMock).enableResponseTimeout(timeoutMsInAck);
        verify(onEnd, never()).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesResponsesRemainingIsDecreased() {
        /*ackTimeout = 0, responseTimeout=200; waitForResponses = 2
        */
        int responseTimeout = 200;
        int responsesRemaining = 2;
        when(messageOptionsMock.getAckTimeout()).thenReturn(0);
        when(messageOptionsMock.getResponseTimeout()).thenReturn(responseTimeout);
        when(messageOptionsMock.getWaitForResponses()).thenReturn(responsesRemaining);

        Callback<Payload> onResponse = mock(Callback.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(messageOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, null);
        ArgumentCaptor<Callback> onMessageCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(consumerMock).subscribe(onMessageCaptor.capture(), any());

        assertEquals(responsesRemaining, collector.responsesRemaining);

        //send first response
        onMessageCaptor.getValue().call(originalMessageWithPayload);
        assertEquals(1, collector.responsesRemaining);
        verify(onEnd, never()).call(anyList());

        //send last response
        onMessageCaptor.getValue().call(originalMessageWithPayload);
        assertEquals(0, collector.responsesRemaining);
        verify(onEnd).call(anyList());
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
