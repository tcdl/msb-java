package io.github.tcdl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.time.Clock;
import java.util.List;

import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.RequestOptions;
import io.github.tcdl.events.EventHandlers;
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

    private static Message requestMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);
    private static Message originalMessageWithPayload = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);
    private static Message originalMessageWithAck = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(TOPIC);

    @Mock
    private MessageFactory messageFactoryMock;

    @Mock
    private RequestOptions requestOptionsMock;

    @Mock
    private ChannelManager channelManagerMock;

    @Mock
    private EventHandlers eventHandlers;

    @Mock
    private MsbConfigurations msbConfigurationsMock;

    @Mock
    private TimeoutManager timeoutManagerMock;

    private MsbContext msbContext;

    @Before
    public void setUp() throws IOException {
        CollectorSubscriber collectorSubscriber = new CollectorSubscriber(channelManagerMock);
        msbContext = new MsbContext(msbConfigurationsMock, messageFactoryMock, channelManagerMock, Clock.systemDefaultZone(), timeoutManagerMock,
                collectorSubscriber);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateCollectorNullMsgOptions() {
        new Collector(null, msbContext, eventHandlers);
    }

    @Test
    public void testGetWaitForResponsesConfigsReturnFalse() {
        when(requestOptionsMock.getWaitForResponses()).thenReturn(0);
        Collector collector = new Collector(requestOptionsMock, msbContext, eventHandlers);
        assertFalse("expect false if MessageOptions.waitForResponses equals 0", collector.isAwaitingResponses());
    }

    @Test
    public void testGetWaitForResponsesConfigsReturnTrue() {
        when(requestOptionsMock.getWaitForResponses()).thenReturn(100);
        Collector collector = new Collector(requestOptionsMock, msbContext, eventHandlers);
        assertTrue("expect true if MessageOptions.waitForResponses equals 100", collector.isAwaitingResponses());
    }

    @Test
    public void testIsAwaitingAcksConfigsNotSetAckTimeoutReturnFalse() {
        when(requestOptionsMock.getAckTimeout()).thenReturn(null);
        Collector collector = new Collector(requestOptionsMock, msbContext, eventHandlers);
        assertFalse("expect false if MessageOptions.ackTimeout null", collector.isAwaitingAcks());
    }

    @Test
    public void testIsAwaitingAcksReturnTrue() {
        when(requestOptionsMock.getAckTimeout()).thenReturn(200);
        Collector collector = new Collector(requestOptionsMock, msbContext, eventHandlers);
        assertTrue("expect true if MessageOptions.ackTimeout equals 200", collector.isAwaitingAcks());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesFilterNull() {
        Callback<Payload> onResponse = mock(Callback.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        Collector collector = spy(new Collector(requestOptionsMock, msbContext, eventHandlers));
        collector.listenForResponses(TOPIC, originalMessageWithPayload);

        ArgumentCaptor<Subscriber> subscriberCaptor = ArgumentCaptor.forClass(Subscriber.class);
        verify(channelManagerMock).subscribe(anyString(), subscriberCaptor.capture());
        subscriberCaptor.getValue().handleMessage(originalMessageWithPayload);

        verify(onResponse).call(originalMessageWithPayload.getPayload());
        assertTrue(collector.getPayloadMessages().contains(originalMessageWithPayload));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesFilterReturnTrue() {
        Callback<Payload> onResponse = mock(Callback.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        Collector collector = spy(new Collector(requestOptionsMock, msbContext, eventHandlers));
        when(collector.acceptMessage(originalMessageWithPayload)).thenReturn(true);
        collector.listenForResponses(TOPIC, originalMessageWithPayload);

        ArgumentCaptor<Subscriber> subscriberCaptor = ArgumentCaptor.forClass(Subscriber.class);
        verify(channelManagerMock).subscribe(anyString(), subscriberCaptor.capture());
        subscriberCaptor.getValue().handleMessage(originalMessageWithPayload);

        verify(collector).acceptMessage(eq(originalMessageWithPayload));
        verify(onResponse).call(originalMessageWithPayload.getPayload());
        assertTrue(collector.getPayloadMessages().contains(originalMessageWithPayload));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesFilterReturnFalse() {
        Callback<Payload> onResponse = mock(Callback.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        Collector collector = spy(new Collector(requestOptionsMock, msbContext, eventHandlers));
        when(collector.acceptMessage(originalMessageWithPayload)).thenReturn(false);
        collector.listenForResponses(TOPIC, originalMessageWithPayload);

        ArgumentCaptor<Subscriber> subscriberCaptor = ArgumentCaptor.forClass(Subscriber.class);
        verify(channelManagerMock).subscribe(anyString(), subscriberCaptor.capture());
        subscriberCaptor.getValue().handleMessage(originalMessageWithPayload);

        verify(collector).acceptMessage(eq(originalMessageWithPayload));
        verify(onResponse, never()).call(originalMessageWithPayload.getPayload());
        assertFalse(collector.getPayloadMessages().contains(originalMessageWithPayload));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesFilterReturnTrueReceivedAck() {
        Callback<Acknowledge> onAck = mock(Callback.class);
        when(eventHandlers.onAcknowledge()).thenReturn(onAck);
        Collector collector = spy(new Collector(requestOptionsMock, msbContext, eventHandlers));
        when(collector.acceptMessage(originalMessageWithPayload)).thenReturn(true);
        collector.listenForResponses(TOPIC, originalMessageWithAck);

        ArgumentCaptor<Subscriber> subscriberCaptor = ArgumentCaptor.forClass(Subscriber.class);
        verify(channelManagerMock).subscribe(anyString(), subscriberCaptor.capture());
        subscriberCaptor.getValue().handleMessage(originalMessageWithAck);

        verify(collector).acceptMessage(eq(originalMessageWithAck));
        verify(onAck).call(originalMessageWithAck.getAck());
        assertTrue(collector.getAckMessages().contains(originalMessageWithAck));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesEndEventNoResponsesRemaining() {
        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = spy(new Collector(requestOptionsMock, msbContext, eventHandlers));
        when(collector.acceptMessage(originalMessageWithPayload)).thenReturn(true);
        collector.listenForResponses(TOPIC, originalMessageWithAck);

        msbContext.getCollectorSubscriber().handleMessage(originalMessageWithAck);

        verify(onEnd).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesLastResponse() {
        /*ackTimeout = 0, responseTimeout=200; waitForResponses = 1
        */
        int responseTimeout = 200;
        when(requestOptionsMock.getAckTimeout()).thenReturn(0);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(responseTimeout);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(1);

        Callback<Payload> onResponse = mock(Callback.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(requestOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, originalMessageWithPayload);

        ArgumentCaptor<Subscriber> subscriberCaptor = ArgumentCaptor.forClass(Subscriber.class);
        verify(channelManagerMock).subscribe(anyString(), subscriberCaptor.capture());
        subscriberCaptor.getValue().handleMessage(originalMessageWithPayload);

        verify(onResponse).call(originalMessageWithPayload.getPayload());
        verify(timeoutManagerMock, never()).enableResponseTimeout(eq(responseTimeout), eq(collector));
        verify(timeoutManagerMock, never()).enableAckTimeout(eq(0), eq(collector));
        verify(onEnd).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesWaitForOneMoreResponse() {
        /*ackTimeout = 0, responseTimeout=200; waitForResponses = 2
        */
        int responseTimeout = 200;
        when(requestOptionsMock.getAckTimeout()).thenReturn(0);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(responseTimeout);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(2);

        Callback<Payload> onResponse = mock(Callback.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(requestOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, originalMessageWithPayload);
        ArgumentCaptor<Subscriber> subscriberCaptor = ArgumentCaptor.forClass(Subscriber.class);
        verify(channelManagerMock).subscribe(anyString(), subscriberCaptor.capture());

        //send first response
        subscriberCaptor.getValue().handleMessage(originalMessageWithPayload);
        verify(onResponse).call(originalMessageWithPayload.getPayload());
        verify(onEnd, never()).call(anyList());

        //send last response
        subscriberCaptor.getValue().handleMessage(originalMessageWithPayload);
        verify(onResponse, times(2)).call(originalMessageWithPayload.getPayload());
        verify(timeoutManagerMock, never()).enableResponseTimeout(eq(responseTimeout), eq(collector));
        verify(timeoutManagerMock, never()).enableAckTimeout(eq(0), eq(collector));
        verify(onEnd).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesNoResponsesRemainingButAwaitAck() {
        /*ackTimeout = 100, responseTimeout=0; waitForResponses = 0
        */
        int ackTimeoutMs = 100;
        when(requestOptionsMock.getAckTimeout()).thenReturn(ackTimeoutMs);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(0);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(0);

        Callback<Acknowledge> onAck = mock(Callback.class);
        when(eventHandlers.onAcknowledge()).thenReturn(onAck);
        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(requestOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, originalMessageWithPayload);
        ArgumentCaptor<Subscriber> subscriberCaptor = ArgumentCaptor.forClass(Subscriber.class);
        verify(channelManagerMock).subscribe(anyString(), subscriberCaptor.capture());

        //send payload response
        subscriberCaptor.getValue().handleMessage(originalMessageWithPayload);
        verify(timeoutManagerMock, never()).enableResponseTimeout(eq(0), eq(collector));
        verify(timeoutManagerMock).enableAckTimeout(anyInt(), eq(collector));
        verify(onEnd, never()).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesReceivedPayloadButAwaitAck() {
        /*ackTimeout = 100, responseTimeout=0; waitForResponses = 1
        */
        int ackTimeoutMs = 100;
        when(requestOptionsMock.getAckTimeout()).thenReturn(ackTimeoutMs);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(0);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(1);

        Callback<Acknowledge> onAck = mock(Callback.class);
        when(eventHandlers.onAcknowledge()).thenReturn(onAck);
        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(requestOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, originalMessageWithPayload);
        ArgumentCaptor<Subscriber> subscriberCaptor = ArgumentCaptor.forClass(Subscriber.class);
        verify(channelManagerMock).subscribe(anyString(), subscriberCaptor.capture());

        //send payload response
        subscriberCaptor.getValue().handleMessage(originalMessageWithPayload);
        verify(timeoutManagerMock, never()).enableResponseTimeout(eq(0), eq(collector));
        verify(timeoutManagerMock).enableAckTimeout(anyInt(), eq(collector));
        verify(onEnd, never()).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesNoResponsesRemainingAndWaitUntilAckBeforeNow() {
        /*ackTimeout = 0, responseTimeout=0; waitForResponses = 0
        */
        int ackTimeoutMs = 0;
        when(requestOptionsMock.getAckTimeout()).thenReturn(ackTimeoutMs);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(0);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(0);

        Callback<Acknowledge> onAck = mock(Callback.class);
        when(eventHandlers.onAcknowledge()).thenReturn(onAck);
        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(requestOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, originalMessageWithPayload);
        ArgumentCaptor<Subscriber> subscriberCaptor = ArgumentCaptor.forClass(Subscriber.class);
        verify(channelManagerMock).subscribe(anyString(), subscriberCaptor.capture());

        //send payload response
        subscriberCaptor.getValue().handleMessage(originalMessageWithPayload);
        verify(timeoutManagerMock, never()).enableResponseTimeout(eq(0), eq(collector));
        verify(timeoutManagerMock, never()).enableAckTimeout(eq(ackTimeoutMs), eq(collector));
        verify(onEnd).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesReceivedAckWithSameTimeoutValue() {
         /*ackTimeout = 0, responseTimeout= 50; waitForResponses = 0
        */
        int timeoutMs = 50;
        when(requestOptionsMock.getAckTimeout()).thenReturn(0);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(timeoutMs);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(0);

        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(requestOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, requestMessage);

        Acknowledge ack = new Acknowledge.AcknowledgeBuilder().withResponderId(Utils.generateId()).withResponsesRemaining(0).withTimeoutMs(timeoutMs).build();
        Message messageWithAck = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(ack, TOPIC, requestMessage.getCorrelationId());

        ArgumentCaptor<Subscriber> subscriberCaptor = ArgumentCaptor.forClass(Subscriber.class);
        verify(channelManagerMock).subscribe(anyString(), subscriberCaptor.capture());
        subscriberCaptor.getValue().handleMessage(messageWithAck);

        verify(timeoutManagerMock, never()).enableResponseTimeout(eq(timeoutMs), eq(collector));
        verify(onEnd).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesReceivedAckWithUpdatedTimeoutAndNoResponsesRemaining() {
         /*ackTimeout = 0, responseTimeout= 50; waitForResponses = 0
        */
        int timeoutMs = 50;
        int timeoutMsInAck = 100;
        when(requestOptionsMock.getAckTimeout()).thenReturn(0);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(timeoutMs);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(0);

        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(requestOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, requestMessage);

        Acknowledge ack = new Acknowledge.AcknowledgeBuilder().withResponderId(Utils.generateId()).withResponsesRemaining(0).withTimeoutMs(timeoutMsInAck)
                .build();
        Message messageWithAck = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(ack, TOPIC, requestMessage.getCorrelationId());

        ArgumentCaptor<Subscriber> subscriberCaptor = ArgumentCaptor.forClass(Subscriber.class);
        verify(channelManagerMock).subscribe(anyString(), subscriberCaptor.capture());
        subscriberCaptor.getValue().handleMessage(messageWithAck);

        verify(timeoutManagerMock).enableResponseTimeout(eq(timeoutMsInAck), eq(collector));
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
        when(requestOptionsMock.getAckTimeout()).thenReturn(0);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(timeoutMs);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(0);

        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(requestOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, requestMessage);

        Acknowledge ack = new Acknowledge.AcknowledgeBuilder().withResponderId(Utils.generateId()).withResponsesRemaining(responsesRemaining)
                .withTimeoutMs(timeoutMsInAck).build();
        Message messageWithAck = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(ack, TOPIC, requestMessage.getCorrelationId());

        ArgumentCaptor<Subscriber> subscriberCaptor = ArgumentCaptor.forClass(Subscriber.class);
        verify(channelManagerMock).subscribe(anyString(), subscriberCaptor.capture());
        subscriberCaptor.getValue().handleMessage(messageWithAck);

        verify(timeoutManagerMock).enableResponseTimeout(eq(timeoutMsInAck), eq(collector));
        verify(onEnd, never()).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesReceivedAcksWithUpdatedTimeoutAndResponsesRemaining() {
        /*ackTimeout = 0, responseTimeout= 50; waitForResponses = 2
        */
        int timeoutMs = 50;
        int timeoutMsInAckResponderOne = 100;
        int responsesRemainingResponderOne = 5;
        int timeoutMsInAckResponderTwo = 222;
        int responsesRemainingResponderTwo = 7;
        when(requestOptionsMock.getAckTimeout()).thenReturn(0);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(timeoutMs);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(0);

        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(requestOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses(TOPIC, requestMessage);

        Acknowledge ackRespOne = new Acknowledge.AcknowledgeBuilder().withResponderId(Utils.generateId()).withResponsesRemaining(responsesRemainingResponderOne)
                .withTimeoutMs(timeoutMsInAckResponderOne).build();
        Message messageWithAckOne = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(ackRespOne, TOPIC, requestMessage.getCorrelationId());

        Acknowledge ackRespTwo = new Acknowledge.AcknowledgeBuilder().withResponderId(Utils.generateId()).withResponsesRemaining(responsesRemainingResponderTwo)
                .withTimeoutMs(timeoutMsInAckResponderTwo).build();
        Message messageWithAckTwo = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(ackRespTwo, TOPIC, requestMessage.getCorrelationId());

        ArgumentCaptor<Subscriber> subscriberCaptor = ArgumentCaptor.forClass(Subscriber.class);
        verify(channelManagerMock).subscribe(anyString(), subscriberCaptor.capture());

        subscriberCaptor.getValue().handleMessage(messageWithAckOne);
        verify(timeoutManagerMock).enableResponseTimeout(eq(timeoutMsInAckResponderOne), eq(collector));
        assertEquals(responsesRemainingResponderOne, collector.getResponsesRemaining());
        verify(onEnd, never()).call(anyList());

        subscriberCaptor.getValue().handleMessage(messageWithAckTwo);
        verify(timeoutManagerMock, times(1)).enableResponseTimeout(eq(timeoutMsInAckResponderTwo), eq(collector));
        assertEquals(responsesRemainingResponderOne + responsesRemainingResponderTwo, collector.getResponsesRemaining());
        verify(onEnd, never()).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesEnsureResponsesRemainingIsDecreased() {
        /*ackTimeout = 0, responseTimeout=200; waitForResponses = 2
        */
        int responseTimeout = 200;
        int responsesRemaining = 2;
        when(requestOptionsMock.getAckTimeout()).thenReturn(0);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(responseTimeout);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(responsesRemaining);

        Callback<Payload> onResponse = mock(Callback.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = spy(new Collector(requestOptionsMock, msbContext, eventHandlers));
        collector.listenForResponses(TOPIC, originalMessageWithPayload);
        ArgumentCaptor<Subscriber> subscriberCaptor = ArgumentCaptor.forClass(Subscriber.class);
        verify(channelManagerMock).subscribe(anyString(), subscriberCaptor.capture());

        assertEquals(responsesRemaining, collector.getResponsesRemaining());

        //send first response
        subscriberCaptor.getValue().handleMessage(originalMessageWithPayload);
        assertEquals(1, collector.getResponsesRemaining());
        verify(onEnd, never()).call(anyList());

        //send last response
        subscriberCaptor.getValue().handleMessage(originalMessageWithPayload);
        assertEquals(0, collector.getResponsesRemaining());
        verify(onEnd).call(anyList());
    }
}
