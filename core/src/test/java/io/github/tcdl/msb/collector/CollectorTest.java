package io.github.tcdl.msb.collector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.time.Clock;
import java.util.List;

import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.events.EventHandlers;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.message.MessageFactory;
import io.github.tcdl.msb.support.TestUtils;
import io.github.tcdl.msb.support.Utils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Created by rdro on 4/27/2015.
 */
@RunWith(MockitoJUnitRunner.class)
public class CollectorTest {

    private static final String TOPIC = "test:collector";

    private static Message originalMessageWithPayload = TestUtils.createMsbRequestMessageWithSimplePayload(TOPIC);
    private static Message originalMessageWithAck = TestUtils.createMsbRequestMessageWithAckNoPayload(TOPIC);

    @Mock
    private MessageFactory messageFactoryMock;

    @Mock
    private RequestOptions requestOptionsMock;

    @Mock
    private ChannelManager channelManagerMock;

    @Mock
    private EventHandlers eventHandlers;

    @Mock
    private MsbConfig msbConfigurationsMock;

    @Mock
    private TimeoutManager timeoutManagerMock;

    @Mock
    private CollectorManagerFactory collectorManagerFactoryMock;

    @Mock
    private CollectorManager collectorManagerMock;

    private MsbContextImpl msbContext;

    @Before
    public void setUp() throws IOException {
        msbContext = TestUtils.createMsbContextBuilder()
                .withMsbConfigurations(msbConfigurationsMock)
                .withMessageFactory(messageFactoryMock)
                .withChannelManager(channelManagerMock)
                .withClock(Clock.systemDefaultZone())
                .withTimeoutManager(timeoutManagerMock)
                .withCollectorManagerFactory(collectorManagerFactoryMock)
                .build();

        when(collectorManagerFactoryMock.findOrCreateCollectorManager(TOPIC)).thenReturn(collectorManagerMock);
        when(requestOptionsMock.getPayloadClass()).thenReturn(Payload.class);
    }

    @Test
    public void testGetWaitForResponsesConfigsReturnFalse() {
        when(requestOptionsMock.getWaitForResponses()).thenReturn(0);
        Collector collector = new Collector(TOPIC, originalMessageWithPayload, requestOptionsMock, msbContext, eventHandlers);
        assertFalse("expect false if MessageOptions.waitForResponses equals 0", collector.isAwaitingResponses());
    }

    @Test
    public void testGetWaitForResponsesConfigsReturnFalseMinusCase() {
        when(requestOptionsMock.getWaitForResponses()).thenReturn(-10);
        Collector collector = new Collector(TOPIC, originalMessageWithPayload, requestOptionsMock, msbContext, eventHandlers);
        assertFalse("expect false if MessageOptions.waitForResponses equals -10", collector.isAwaitingResponses());
    }

    @Test
    public void testGetWaitForResponsesConfigsReturnTrue() {
        when(requestOptionsMock.getWaitForResponses()).thenReturn(100);
        Collector collector = new Collector(TOPIC, originalMessageWithPayload, requestOptionsMock, msbContext, eventHandlers);
        assertTrue("expect true if MessageOptions.waitForResponses equals 100", collector.isAwaitingResponses());
    }

    @Test
    public void testGetWaitForResponsesConfigsReturnTrueMinusOne() {
        when(requestOptionsMock.getWaitForResponses()).thenReturn(-1);
        Collector collector = new Collector(TOPIC, originalMessageWithPayload, requestOptionsMock, msbContext, eventHandlers);
        assertTrue("expect true if MessageOptions.waitForResponses equals -1", collector.isAwaitingResponses());
    }

    @Test
    public void testIsAwaitingAcksConfigsNotSetAckTimeoutReturnFalse() {
        when(requestOptionsMock.getAckTimeout()).thenReturn(null);
        Collector collector = new Collector(TOPIC, originalMessageWithPayload, requestOptionsMock, msbContext, eventHandlers);
        assertFalse("expect false if MessageOptions.ackTimeout null", collector.isAwaitingAcks());
    }

    @Test
    public void testIsAwaitingAcksReturnTrue() {
        when(requestOptionsMock.getAckTimeout()).thenReturn(200);
        Collector collector = new Collector(TOPIC, originalMessageWithPayload, requestOptionsMock, msbContext, eventHandlers);
        assertTrue("expect true if MessageOptions.ackTimeout equals 200", collector.isAwaitingAcks());
    }

    @Test
    public void testHandleResponse() {
        String bodyText = "some body";
        Message incomingMessage = TestUtils.createMsbRequestMessageWithPayloadTextBody(TOPIC, bodyText);
        @SuppressWarnings("unchecked")
        Callback<Payload> onResponse = mock(Callback.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        Collector collector = new Collector(TOPIC, incomingMessage, requestOptionsMock, msbContext, eventHandlers);

        // method under test
        collector.handleMessage(incomingMessage);

        Payload<?, ?, ?, String> expectedPayload = new Payload.Builder<Object, Object, Object, String>()
                .withBody(bodyText)
                .build();
        verify(onResponse).call(expectedPayload);
        verify(collectorManagerMock).unsubscribe(collector);
        assertTrue(collector.getPayloadMessages().stream().anyMatch(message -> message.getId().equals(incomingMessage.getId())));
        assertFalse(collector.getAckMessages().contains(incomingMessage));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseReceivedAck() {
        Callback<Acknowledge> onAck = mock(Callback.class);
        when(eventHandlers.onAcknowledge()).thenReturn(onAck);
        Collector collector = new Collector(TOPIC, originalMessageWithPayload, requestOptionsMock, msbContext, eventHandlers);

        collector.handleMessage(originalMessageWithAck);

        verify(onAck).call(originalMessageWithAck.getAck());
        assertTrue(collector.getAckMessages().contains(originalMessageWithAck));
        assertFalse(collector.getPayloadMessages().contains(originalMessageWithPayload));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseEndEventNoResponsesRemaining() {
        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);
        Collector collector = new Collector(TOPIC, originalMessageWithPayload, requestOptionsMock, msbContext, eventHandlers);

        collector.handleMessage(originalMessageWithAck);

        verify(timeoutManagerMock, never()).enableResponseTimeout(anyInt(), any(Collector.class));
        verify(timeoutManagerMock, never()).enableAckTimeout(anyInt(), any(Collector.class));
        verify(onEnd).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseLastResponse() {
        /*ackTimeout = 0, responseTimeout=200; waitForResponses = 1
        */
        String bodyText = "some body";
        Message incomingMessage = TestUtils.createMsbRequestMessageWithPayloadTextBody(TOPIC, bodyText);

        int responseTimeout = 200;
        when(requestOptionsMock.getAckTimeout()).thenReturn(0);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(responseTimeout);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(1);

        Callback<Payload> onResponse = mock(Callback.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(TOPIC, incomingMessage, requestOptionsMock, msbContext, eventHandlers);
        collector.handleMessage(incomingMessage);

        Payload<?, ?, ?, String> expectedPayload = new Payload.Builder<Object, Object, Object, String>()
                .withBody(bodyText)
                .build();
        verify(onResponse).call(expectedPayload);
        verify(timeoutManagerMock, never()).enableResponseTimeout(eq(responseTimeout), eq(collector));
        verify(timeoutManagerMock, never()).enableAckTimeout(eq(0), eq(collector));
        verify(onEnd).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseWaitForOneMoreResponse() {
        /*ackTimeout = 0, responseTimeout=200; waitForResponses = 2
        */
        String bodyText = "some body";
        Message incomingMessage = TestUtils.createMsbRequestMessageWithPayloadTextBody(TOPIC, bodyText);
        int responseTimeout = 200;
        when(requestOptionsMock.getAckTimeout()).thenReturn(0);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(responseTimeout);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(2);

        Callback<Payload> onResponse = mock(Callback.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(TOPIC, incomingMessage, requestOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses();

        Payload<?, ?, ?, String> expectedPayload = new Payload.Builder<Object, Object, Object, String>()
                .withBody(bodyText)
                .build();

        //send first response
        collector.handleMessage(incomingMessage);
        verify(onResponse).call(expectedPayload);
        verify(onEnd, never()).call(anyList());

        //send last response
        collector.handleMessage(incomingMessage);
        verify(onResponse, times(2)).call(expectedPayload);
        verify(timeoutManagerMock, never()).enableResponseTimeout(eq(responseTimeout), eq(collector));
        verify(timeoutManagerMock, never()).enableAckTimeout(eq(0), eq(collector));
        verify(onEnd).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseNoResponsesRemainingButAwaitAck() {
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

        Collector collector = new Collector(TOPIC, originalMessageWithPayload, requestOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses();

        //send payload response
        collector.handleMessage(originalMessageWithPayload);
        verify(timeoutManagerMock, never()).enableResponseTimeout(anyInt(), eq(collector));
        verify(timeoutManagerMock).enableAckTimeout(anyInt(), eq(collector));
        verify(onEnd, never()).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseReceivedPayloadButAwaitAck() {
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

        Collector collector = new Collector(TOPIC, originalMessageWithPayload, requestOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses();

        //send payload response
        collector.handleMessage(originalMessageWithPayload);
        verify(timeoutManagerMock, never()).enableResponseTimeout(eq(0), eq(collector));
        verify(timeoutManagerMock).enableAckTimeout(anyInt(), eq(collector));
        verify(onEnd, never()).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseNoResponsesRemainingAndWaitUntilAckBeforeNow() {
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

        Collector collector = new Collector(TOPIC, originalMessageWithPayload, requestOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses();

        //send payload response
        collector.handleMessage(originalMessageWithPayload);
        verify(timeoutManagerMock, never()).enableResponseTimeout(eq(0), eq(collector));
        verify(timeoutManagerMock, never()).enableAckTimeout(eq(ackTimeoutMs), eq(collector));
        verify(onEnd).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseReceivedAckWithSameTimeoutValue() {
         /*ackTimeout = 0, responseTimeout= 50; waitForResponses = 0
        */
        int timeoutMs = 50;
        when(requestOptionsMock.getAckTimeout()).thenReturn(0);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(timeoutMs);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(0);

        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(TOPIC, originalMessageWithPayload, requestOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses();

        Acknowledge ack = new Acknowledge.Builder().withResponderId(Utils.generateId()).withResponsesRemaining(0).withTimeoutMs(timeoutMs).build();
        Message messageWithAck = TestUtils.createMsbRequestMessageNoPayload(ack, TOPIC, originalMessageWithPayload.getCorrelationId());

        collector.handleMessage(messageWithAck);

        verify(timeoutManagerMock, never()).enableResponseTimeout(eq(timeoutMs), eq(collector));
        verify(onEnd).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseReceivedAckWithUpdatedTimeoutAndNoResponsesRemaining() {
         /*ackTimeout = 0, responseTimeout= 50; waitForResponses = 0
        */
        int timeoutMs = 50;
        int timeoutMsInAck = 100;
        when(requestOptionsMock.getAckTimeout()).thenReturn(0);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(timeoutMs);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(0);

        Callback<List<Message>> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(TOPIC, originalMessageWithPayload, requestOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses();

        Acknowledge ack = new Acknowledge.Builder().withResponderId(Utils.generateId()).withResponsesRemaining(0).withTimeoutMs(timeoutMsInAck)
                .build();
        Message messageWithAck = TestUtils.createMsbRequestMessageNoPayload(ack, TOPIC, originalMessageWithPayload.getCorrelationId());

        collector.handleMessage(messageWithAck);

        verify(timeoutManagerMock).enableResponseTimeout(eq(timeoutMsInAck), eq(collector));
        verify(onEnd).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseReceivedAckWithUpdatedTimeoutAndResponsesRemaining() {
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

        Collector collector = new Collector(TOPIC, originalMessageWithPayload, requestOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses();

        Acknowledge ack = new Acknowledge.Builder().withResponderId(Utils.generateId()).withResponsesRemaining(responsesRemaining)
                .withTimeoutMs(timeoutMsInAck).build();
        Message messageWithAck = TestUtils.createMsbRequestMessageNoPayload(ack, TOPIC, originalMessageWithPayload.getCorrelationId());

        collector.handleMessage(messageWithAck);

        verify(timeoutManagerMock).enableResponseTimeout(eq(timeoutMsInAck), eq(collector));
        verify(onEnd, never()).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseReceivedAcksWithUpdatedTimeoutAndResponsesRemaining() {
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

        Collector collector = new Collector(TOPIC, originalMessageWithPayload, requestOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses();

        Acknowledge ackRespOne = new Acknowledge.Builder().withResponderId(Utils.generateId()).withResponsesRemaining(responsesRemainingResponderOne)
                .withTimeoutMs(timeoutMsInAckResponderOne).build();
        Message messageWithAckOne = TestUtils
                .createMsbRequestMessageNoPayload(ackRespOne, TOPIC, originalMessageWithPayload.getCorrelationId());

        Acknowledge ackRespTwo = new Acknowledge.Builder().withResponderId(Utils.generateId()).withResponsesRemaining(responsesRemainingResponderTwo)
                .withTimeoutMs(timeoutMsInAckResponderTwo).build();
        Message messageWithAckTwo = TestUtils
                .createMsbRequestMessageNoPayload(ackRespTwo, TOPIC, originalMessageWithPayload.getCorrelationId());

        collector.handleMessage(messageWithAckOne);
        verify(timeoutManagerMock).enableResponseTimeout(eq(timeoutMsInAckResponderOne), eq(collector));
        assertEquals(responsesRemainingResponderOne, collector.getResponsesRemaining());
        verify(onEnd, never()).call(anyList());

        collector.handleMessage(messageWithAckTwo);
        verify(timeoutManagerMock, times(1)).enableResponseTimeout(eq(timeoutMsInAckResponderTwo), eq(collector));
        assertEquals(responsesRemainingResponderOne + responsesRemainingResponderTwo, collector.getResponsesRemaining());
        verify(onEnd, never()).call(anyList());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseEnsureResponsesRemainingIsDecreased() {
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

        Collector collector = new Collector(TOPIC, originalMessageWithPayload, requestOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses();

        assertEquals(responsesRemaining, collector.getResponsesRemaining());

        //send first response
        collector.handleMessage(originalMessageWithPayload);
        assertEquals(1, collector.getResponsesRemaining());
        verify(onEnd, never()).call(anyList());

        //send last response
        collector.handleMessage(originalMessageWithPayload);
        assertEquals(0, collector.getResponsesRemaining());
        verify(onEnd).call(anyList());
    }

    @Test
    public void testListenForResponses() {
        Collector collector = new Collector(TOPIC, originalMessageWithPayload, requestOptionsMock, msbContext, eventHandlers);
        collector.listenForResponses();

        verify(collectorManagerMock).registerCollector(collector);
    }
}