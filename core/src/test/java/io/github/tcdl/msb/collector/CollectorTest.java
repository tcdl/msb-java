package io.github.tcdl.msb.collector;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.exception.JsonConversionException;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.RestPayload;
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

import java.io.IOException;
import java.time.Clock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class CollectorTest {

    private static final String TOPIC = "test:collector";
    private static final String TOPIC_RESPONSE = "test:collector:response:12345";

    private static Message originalMessage = TestUtils.createSimpleRequestMessage(TOPIC);
    private static Message responseMessageWithAck = TestUtils.createMsbResponseMessageWithAckNoPayload(TOPIC_RESPONSE);

    @Mock
    private MessageFactory messageFactoryMock;

    @Mock
    private RequestOptions requestOptionsMock;

    @Mock
    private ChannelManager channelManagerMock;

    @Mock
    private EventHandlers<RestPayload> eventHandlers;

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
    }

    @Test
    public void testIsAwaitingResponsesConfigsReturnNull() {
        when(requestOptionsMock.getWaitForResponses()).thenReturn(null);
        Collector<RestPayload> collector = new Collector<>(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, new TypeReference<RestPayload>() {});
        assertTrue("expect true if MessageOptions.waitForResponses is null", collector.isAwaitingResponses());
    }

    @Test
    public void testIsAwaitingResponsesConfigsReturnZero() {
        when(requestOptionsMock.getWaitForResponses()).thenReturn(0);
        Collector<RestPayload> collector = new Collector<>(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, new TypeReference<RestPayload>() {});
        assertFalse("expect false if MessageOptions.waitForResponses equals 0", collector.isAwaitingResponses());
    }

    @Test
    public void testIsAwaitingResponsesConfigsReturnMinusOne() {
        when(requestOptionsMock.getWaitForResponses()).thenReturn(-1);
        Collector<RestPayload> collector = new Collector<>(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, new TypeReference<RestPayload>() {});
        assertTrue("expect true if MessageOptions.waitForResponses equals -1", collector.isAwaitingResponses());
    }

    @Test
    public void testIsAwaitingResponsesConfigsReturnPositive() {
        when(requestOptionsMock.getWaitForResponses()).thenReturn(100);
        Collector<RestPayload> collector = new Collector<>(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, new TypeReference<RestPayload>() {});
        assertTrue("expect true if MessageOptions.waitForResponses equals 100", collector.isAwaitingResponses());
    }

    @Test
    public void testIsAwaitingAcksConfigsReturnPositiveValue() {
        when(requestOptionsMock.getAckTimeout()).thenReturn(1000);
        Collector<RestPayload> collector = new Collector<>(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, new TypeReference<RestPayload>() {});
        collector.listenForResponses();
        assertTrue("expect true if MessageOptions.ackTimeout equals 1000", collector.isAwaitingAcks());
    }

    @Test
    public void testIsAwaitingAcksConfigsReturnNull() {
        when(requestOptionsMock.getAckTimeout()).thenReturn(null);
        Collector<RestPayload> collector = new Collector<>(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, new TypeReference<RestPayload>() {});
        collector.listenForResponses();
        assertFalse("expect false if MessageOptions.ackTimeout null", collector.isAwaitingAcks());
    }

    @Test
    public void testIsAwaitingAcksConfigsReturnZero() {
        when(requestOptionsMock.getAckTimeout()).thenReturn(0);
        Collector<RestPayload> collector = new Collector<>(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, new TypeReference<RestPayload>() {});
        collector.listenForResponses();
        assertFalse("expect false if MessageOptions.ackTimeout=0", collector.isAwaitingAcks());
    }

    @Test
    public void testHandleResponse() {
        String bodyText =  "some body";
        Message responseMessage = TestUtils.createMsbRequestMessage(TOPIC, bodyText);
        @SuppressWarnings("unchecked")
        Callback<RestPayload> onResponse = mock(Callback.class);
        @SuppressWarnings("unchecked")
        Callback<Message> onRawResponse = mock(Callback.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        when(eventHandlers.onRawResponse()).thenReturn(onRawResponse);
        Collector<RestPayload> collector = new Collector<>(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, new TypeReference<RestPayload>() {});

        // method under test
        collector.handleMessage(responseMessage);

        RestPayload<?, ?, ?, String> expectedPayload = new RestPayload.Builder<Object, Object, Object, String>()
                .withBody(bodyText)
                .build();
        verify(onRawResponse).call(responseMessage);
        verify(onResponse).call(expectedPayload);
        verify(collectorManagerMock).unregisterCollector(collector);
        assertTrue(collector.getPayloadMessages().stream().anyMatch(message -> message.getId().equals(responseMessage.getId())));
        assertFalse(collector.getAckMessages().contains(responseMessage));
    }

    @Test(expected = JsonConversionException.class)
    public void testHandleResponseConversionFailed() {
        String bodyText = "some body";
        Message responseMessage = TestUtils.createMsbRequestMessage(TOPIC, bodyText);
        @SuppressWarnings("unchecked")
        Callback<RestPayload<?, ?, ?, Integer>> onResponse = mock(Callback.class);
        @SuppressWarnings("unchecked")
        Callback<Message> onRawResponse = mock(Callback.class);

        @SuppressWarnings("unchecked")
        EventHandlers<RestPayload<?, ?, ?, Integer>> eventHandlers = mock(EventHandlers.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        when(eventHandlers.onRawResponse()).thenReturn(onRawResponse);
        TypeReference<RestPayload<?, ?, ?, Integer>> payloadTypeReference = new TypeReference<RestPayload<?, ?, ?, Integer>>() {};
        Collector<RestPayload<?, ?, ?, Integer>> collector = new Collector<>(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, payloadTypeReference);

        // make sure that onRawResponse is called even if conversion of payload to custom type fails
        try {
            collector.handleMessage(responseMessage);
        } finally {
            verify(onRawResponse).call(responseMessage);
            verify(onResponse, never()).call(any());
        }
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseReceivedAck() {
        Callback<Acknowledge> onAck = mock(Callback.class);
        when(eventHandlers.onAcknowledge()).thenReturn(onAck);
        Collector collector = new Collector(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, new TypeReference<RestPayload>() {});

        collector.handleMessage(responseMessageWithAck);

        verify(onAck).call(responseMessageWithAck.getAck());
        assertTrue(collector.getAckMessages().contains(responseMessageWithAck));
        assertFalse(collector.getPayloadMessages().contains(responseMessageWithAck));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseEndEventNoResponsesRemaining() {
        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);
        Collector collector = new Collector(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, new TypeReference<RestPayload>() {});

        collector.handleMessage(responseMessageWithAck);

        verify(timeoutManagerMock, never()).enableResponseTimeout(anyInt(), any(Collector.class));
        verify(timeoutManagerMock, never()).enableAckTimeout(anyInt(), any(Collector.class));
        verify(onEnd).call(any());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseLastResponse() {
        String bodyText = "some body";
        Message responseMessage = TestUtils.createMsbRequestMessage(TOPIC, bodyText);

        /*ackTimeout = 0, responseTimeout=200; waitForResponses = 1
        */
        int responseTimeout = 200;
        when(requestOptionsMock.getAckTimeout()).thenReturn(0);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(responseTimeout);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(1);

        Callback<RestPayload> onResponse = mock(Callback.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, new TypeReference<RestPayload>() {});
        collector.handleMessage(responseMessage);

        RestPayload<?, ?, ?, String> expectedPayload = new RestPayload.Builder<Object, Object, Object, String>()
                .withBody(bodyText)
                .build();
        verify(onResponse).call(expectedPayload);
        verify(timeoutManagerMock, never()).enableResponseTimeout(eq(responseTimeout), eq(collector));
        verify(timeoutManagerMock, never()).enableAckTimeout(eq(0), eq(collector));
        verify(onEnd).call(any());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseWaitForOneMoreResponse() {
        String bodyText = "some body";
        Message responseMessage = TestUtils.createMsbRequestMessage(TOPIC, bodyText);

        /*ackTimeout = 0, responseTimeout=200; waitForResponses = 2
        */
        int responseTimeout = 200;
        when(requestOptionsMock.getAckTimeout()).thenReturn(0);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(responseTimeout);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(2);

        Callback<RestPayload> onResponse = mock(Callback.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, new TypeReference<RestPayload>() {});
        collector.listenForResponses();

        RestPayload<?, ?, ?, String> expectedPayload = new RestPayload.Builder<Object, Object, Object, String>()
                .withBody(bodyText)
                .build();

        //send first response
        collector.handleMessage(responseMessage);
        verify(onResponse).call(expectedPayload);
        verify(onEnd, never()).call(any());

        //send last response
        collector.handleMessage(responseMessage);
        verify(onResponse, times(2)).call(expectedPayload);
        verify(timeoutManagerMock, never()).enableResponseTimeout(eq(responseTimeout), eq(collector));
        verify(timeoutManagerMock, never()).enableAckTimeout(eq(0), eq(collector));
        verify(onEnd).call(any());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseNoResponsesRemainingButAwaitAck() {
        String bodyText = "some body";
        Message responseMessage = TestUtils.createMsbRequestMessage(TOPIC, bodyText);

        /*ackTimeout = 100, responseTimeout=0; waitForResponses = 0
        */
        int ackTimeoutMs = 100;
        when(requestOptionsMock.getAckTimeout()).thenReturn(ackTimeoutMs);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(0);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(0);

        Callback<Acknowledge> onAck = mock(Callback.class);
        when(eventHandlers.onAcknowledge()).thenReturn(onAck);
        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, new TypeReference<RestPayload>() {});
        collector.listenForResponses();

        //send payload response
        collector.handleMessage(responseMessage);
        verify(timeoutManagerMock, never()).enableResponseTimeout(anyInt(), eq(collector));
        verify(timeoutManagerMock).enableAckTimeout(anyInt(), eq(collector));
        verify(onEnd, never()).call(any());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseReceivedPayloadButAwaitAck() {
        String bodyText = "some body";
        Message responseMessage = TestUtils.createMsbRequestMessage(TOPIC, bodyText);

        /*ackTimeout = 100, responseTimeout=0; waitForResponses = 1
        */
        int ackTimeoutMs = 100;
        when(requestOptionsMock.getAckTimeout()).thenReturn(ackTimeoutMs);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(0);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(1);

        Callback<Acknowledge> onAck = mock(Callback.class);
        when(eventHandlers.onAcknowledge()).thenReturn(onAck);
        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, new TypeReference<RestPayload>() {});
        collector.listenForResponses();

        //send payload response
        collector.handleMessage(responseMessage);
        verify(timeoutManagerMock, never()).enableResponseTimeout(eq(0), eq(collector));
        verify(timeoutManagerMock).enableAckTimeout(anyInt(), eq(collector));
        verify(onEnd, never()).call(any());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseNoResponsesRemainingAndWaitUntilAckBeforeNow() {
        String bodyText = "some body";
        Message responseMessage = TestUtils.createMsbRequestMessage(TOPIC, bodyText);

        /*ackTimeout = 0, responseTimeout=0; waitForResponses = 0
        */
        int ackTimeoutMs = 0;
        when(requestOptionsMock.getAckTimeout()).thenReturn(ackTimeoutMs);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(0);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(0);

        Callback<Acknowledge> onAck = mock(Callback.class);
        when(eventHandlers.onAcknowledge()).thenReturn(onAck);
        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, new TypeReference<RestPayload>() {});
        collector.listenForResponses();

        //send payload response
        collector.handleMessage(responseMessage);
        verify(timeoutManagerMock, never()).enableResponseTimeout(eq(0), eq(collector));
        verify(timeoutManagerMock, never()).enableAckTimeout(eq(ackTimeoutMs), eq(collector));
        verify(onEnd).call(any());
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

        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, new TypeReference<RestPayload>() {});
        collector.listenForResponses();

        Acknowledge ack = new Acknowledge.Builder().withResponderId(Utils.generateId()).withResponsesRemaining(0).withTimeoutMs(timeoutMs).build();
        Message responseMessageWithAck = TestUtils.createMsbResponseMessageWithAckNoPayload(ack, TOPIC, originalMessage.getCorrelationId());

        collector.handleMessage(responseMessageWithAck);

        verify(timeoutManagerMock, never()).enableResponseTimeout(eq(timeoutMs), eq(collector));
        verify(onEnd).call(any());
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

        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, new TypeReference<RestPayload>() {});
        collector.listenForResponses();

        Acknowledge ack = new Acknowledge.Builder().withResponderId(Utils.generateId()).withResponsesRemaining(0).withTimeoutMs(timeoutMsInAck)
                .build();
        Message responseMessageWithAck = TestUtils.createMsbResponseMessageWithAckNoPayload(ack, TOPIC, originalMessage.getCorrelationId());

        collector.handleMessage(responseMessageWithAck);

        verify(timeoutManagerMock).enableResponseTimeout(eq(timeoutMsInAck), eq(collector));
        verify(onEnd).call(any());
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

        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, new TypeReference<RestPayload>() {});
        collector.listenForResponses();

        Acknowledge ack = new Acknowledge.Builder().withResponderId(Utils.generateId()).withResponsesRemaining(responsesRemaining)
                .withTimeoutMs(timeoutMsInAck).build();
        Message responseMessageWithAck = TestUtils.createMsbResponseMessageWithAckNoPayload(ack, TOPIC, originalMessage.getCorrelationId());

        collector.handleMessage(responseMessageWithAck);

        verify(timeoutManagerMock).enableResponseTimeout(eq(timeoutMsInAck), eq(collector));
        verify(onEnd, never()).call(any());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseReceivedAcksWithUpdatedTimeoutAndResponsesRemaining() {
        /*ackTimeout = 0, responseTimeout= 50; waitForResponses = 2
        */
        int timeoutMs = 50;
        int timeoutMsInAckResponderOne = 100;
        int responsesRemainingResponderOne = 5;
        int timeoutMsInAckResponderTwo = 200;
        int responsesRemainingResponderTwo = 7;
        when(requestOptionsMock.getAckTimeout()).thenReturn(0);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(timeoutMs);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(0);

        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, new TypeReference<RestPayload>() {});
        collector.listenForResponses();

        Acknowledge ackRespOne = new Acknowledge.Builder().withResponderId(Utils.generateId()).withResponsesRemaining(responsesRemainingResponderOne)
                .withTimeoutMs(timeoutMsInAckResponderOne).build();
        Message messageWithAckOne = TestUtils
                .createMsbResponseMessageWithAckNoPayload(ackRespOne, TOPIC, originalMessage.getCorrelationId());

        Acknowledge ackRespTwo = new Acknowledge.Builder().withResponderId(Utils.generateId()).withResponsesRemaining(responsesRemainingResponderTwo)
                .withTimeoutMs(timeoutMsInAckResponderTwo).build();
        Message messageWithAckTwo = TestUtils
                .createMsbResponseMessageWithAckNoPayload(ackRespTwo, TOPIC, originalMessage.getCorrelationId());

        collector.handleMessage(messageWithAckOne);
        verify(timeoutManagerMock).enableResponseTimeout(eq(timeoutMsInAckResponderOne), eq(collector));
        assertEquals(responsesRemainingResponderOne, collector.getResponsesRemaining());
        verify(onEnd, never()).call(any());

        collector.handleMessage(messageWithAckTwo);
        verify(timeoutManagerMock, times(1)).enableResponseTimeout(eq(timeoutMsInAckResponderTwo), eq(collector));
        assertEquals(responsesRemainingResponderOne + responsesRemainingResponderTwo, collector.getResponsesRemaining());
        verify(onEnd, never()).call(any());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseEnsureResponsesRemainingIsDecreased() {
        String bodyText = "some body";
        Message responseMessage = TestUtils.createMsbRequestMessage(TOPIC, bodyText);

        /*ackTimeout = 0, responseTimeout=200; waitForResponses = 2
        */
        int responseTimeout = 200;
        int responsesRemaining = 2;
        when(requestOptionsMock.getAckTimeout()).thenReturn(0);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(responseTimeout);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(responsesRemaining);

        Callback<RestPayload> onResponse = mock(Callback.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector collector = new Collector(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, new TypeReference<RestPayload>() {});
        collector.listenForResponses();

        assertEquals(responsesRemaining, collector.getResponsesRemaining());

        //send first response
        collector.handleMessage(responseMessage);
        assertEquals(1, collector.getResponsesRemaining());
        verify(onEnd, never()).call(any());

        //send last response
        collector.handleMessage(responseMessage);
        assertEquals(0, collector.getResponsesRemaining());
        verify(onEnd).call(any());
    }

    @Test
    public void testListenForResponses() {
        Collector<RestPayload> collector = new Collector<>(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, new TypeReference<RestPayload>() {});
        collector.listenForResponses();

        verify(collectorManagerMock).registerCollector(collector);
    }
}