package io.github.tcdl.msb.collector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.api.AcknowledgementHandler;
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

import java.io.IOException;
import java.time.Clock;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiConsumer;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
        this.msbContext = TestUtils.createMsbContextBuilder()
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
    public void testIsAwaitingResponsesConfigsReturnMinusOne() {
        when(requestOptionsMock.getWaitForResponses()).thenReturn(-1);
        Collector<RestPayload> collector = createCollector();

        assertTrue("expect true if MessageOptions.waitForResponses equals -1", collector.isAwaitingResponses());
    }

    @Test
    public void testIsAwaitingResponsesConfigsReturnPositive() {
        when(requestOptionsMock.getWaitForResponses()).thenReturn(1000);
        Collector<RestPayload> collector = createCollector();

        assertTrue("expect true if MessageOptions.waitForResponses is positive number", collector.isAwaitingResponses());
    }

    @Test
    public void testIsAwaitingResponsesConfigsReturnZero() {
        when(requestOptionsMock.getWaitForResponses()).thenReturn(0);
        Collector<RestPayload> collector = createCollector();

        assertFalse("expect false if MessageOptions.waitForResponses equals 0", collector.isAwaitingResponses());
    }

    @Test
    public void testIsAwaitingAcksConfigsReturnPositiveValue() {
        when(requestOptionsMock.getAckTimeout()).thenReturn(1000);
        Collector<RestPayload> collector = createCollector();
        collector.listenForResponses();

        assertTrue("expect true if MessageOptions.ackTimeout is positive number", collector.isAwaitingAcks());
    }

    @Test
    public void testIsAwaitingAcksConfigsReturnNull() {
        when(requestOptionsMock.getAckTimeout()).thenReturn(null);
        Collector<RestPayload> collector = createCollector();
        collector.listenForResponses();

        assertFalse("expect false if MessageOptions.ackTimeout null", collector.isAwaitingAcks());
    }

    @Test
    public void testIsAwaitingAcksConfigsReturnZero() {
        when(requestOptionsMock.getAckTimeout()).thenReturn(0);
        Collector<RestPayload> collector = createCollector();
        collector.listenForResponses();

        assertFalse("expect false if MessageOptions.ackTimeout=0", collector.isAwaitingAcks());
    }

    @Test
    public void testHandleResponse() {
        String bodyText = "some body";
        Message responseMessage = TestUtils.createMsbRequestMessage(TOPIC, bodyText);
        @SuppressWarnings("unchecked")
        BiConsumer<RestPayload, AcknowledgementHandler> onResponse = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        BiConsumer<Message, AcknowledgementHandler> onRawResponse = mock(BiConsumer.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        when(eventHandlers.onRawResponse()).thenReturn(onRawResponse);
        Collector<RestPayload> collector = new Collector<>(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers,
                new TypeReference<RestPayload>() {
                });

        AcknowledgementHandler ackHandler = mock(AcknowledgementHandler.class);
        
        // method under test
        collector.handleMessage(responseMessage, ackHandler);

        RestPayload<?, ?, ?, String> expectedPayload = new RestPayload.Builder<Object, Object, Object, String>()
                .withBody(bodyText)
                .build();
        verify(onRawResponse).accept(responseMessage, ackHandler);
        verify(onResponse).accept(expectedPayload, ackHandler);
        verify(collectorManagerMock).unregisterCollector(collector);
        assertTrue(collector.getPayloadMessages().stream().anyMatch(message -> message.getId().equals(responseMessage.getId())));
        assertFalse(collector.getAckMessages().contains(responseMessage));
    }

    @Test(expected = JsonConversionException.class)
    public void testHandleResponseConversionFailed() {
        String bodyText = "some body";
        Message responseMessage = TestUtils.createMsbRequestMessage(TOPIC, bodyText);
        @SuppressWarnings("unchecked")
        BiConsumer<RestPayload<?, ?, ?, Integer>, AcknowledgementHandler> onResponse = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        BiConsumer<Message, AcknowledgementHandler> onRawResponse = mock(BiConsumer.class);

        @SuppressWarnings("unchecked")
        EventHandlers<RestPayload<?, ?, ?, Integer>> eventHandlers = mock(EventHandlers.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        when(eventHandlers.onRawResponse()).thenReturn(onRawResponse);
        TypeReference<RestPayload<?, ?, ?, Integer>> payloadTypeReference = new TypeReference<RestPayload<?, ?, ?, Integer>>() {
        };
        Collector<RestPayload<?, ?, ?, Integer>> collector = new Collector<>(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers,
                payloadTypeReference);

        AcknowledgementHandler ackHandler = mock(AcknowledgementHandler.class);
        
        // make sure that onRawResponse is called even if conversion of payload to custom type fails
        try {
            collector.handleMessage(responseMessage, ackHandler);
        } finally {
            verify(onRawResponse).accept(responseMessage, ackHandler);
            verify(onResponse, never()).accept(any(), any());
        }
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseReceivedAck() {
        BiConsumer<Acknowledge, AcknowledgementHandler> onAck = mock(BiConsumer.class);
        when(eventHandlers.onAcknowledge()).thenReturn(onAck);
        Collector<RestPayload> collector = createCollector();
        
        AcknowledgementHandler ackHandler = mock(AcknowledgementHandler.class);
        collector.handleMessage(responseMessageWithAck, ackHandler);

        verify(onAck).accept(responseMessageWithAck.getAck(), ackHandler);
        assertTrue(collector.getAckMessages().contains(responseMessageWithAck));
        assertFalse(collector.getPayloadMessages().contains(responseMessageWithAck));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseEndEventNoResponsesRemaining() {
        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);
        Collector<RestPayload> collector = createCollector();

        AcknowledgementHandler ackHandler = mock(AcknowledgementHandler.class);
        collector.handleMessage(responseMessageWithAck, ackHandler);

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

        BiConsumer<RestPayload, AcknowledgementHandler> onResponse = mock(BiConsumer.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector<RestPayload> collector = createCollector();
        AcknowledgementHandler ackHandler = mock(AcknowledgementHandler.class);
        collector.handleMessage(responseMessage, ackHandler);

        RestPayload<?, ?, ?, String> expectedPayload = new RestPayload.Builder<Object, Object, Object, String>()
                .withBody(bodyText)
                .build();
        verify(onResponse).accept(expectedPayload, ackHandler);
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

        BiConsumer<RestPayload, AcknowledgementHandler> onResponse = mock(BiConsumer.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector<RestPayload> collector = createCollector();
        collector.listenForResponses();

        RestPayload<?, ?, ?, String> expectedPayload = new RestPayload.Builder<Object, Object, Object, String>()
                .withBody(bodyText)
                .build();

        AcknowledgementHandler ackHandler = mock(AcknowledgementHandler.class);
        //send first response
        collector.handleMessage(responseMessage, ackHandler);
        verify(onResponse).accept(expectedPayload, ackHandler);
        verify(onEnd, never()).call(any());

        //send last response
        collector.handleMessage(responseMessage, ackHandler);
        verify(onResponse, times(2)).accept(expectedPayload, ackHandler);
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

        BiConsumer<Acknowledge, AcknowledgementHandler> onAck = mock(BiConsumer.class);
        
        when(eventHandlers.onAcknowledge()).thenReturn(onAck);
        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector<RestPayload> collector = createCollector();
        collector.listenForResponses();

        AcknowledgementHandler ackHandler = mock(AcknowledgementHandler.class);
        //send payload response
        collector.handleMessage(responseMessage, ackHandler);
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

        BiConsumer<Acknowledge, AcknowledgementHandler> onAck = mock(BiConsumer.class);
        when(eventHandlers.onAcknowledge()).thenReturn(onAck);
        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector<RestPayload> collector = createCollector();
        collector.listenForResponses();

        AcknowledgementHandler ackHandler = mock(AcknowledgementHandler.class);
        //send payload response
        collector.handleMessage(responseMessage, ackHandler);
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

        BiConsumer<Acknowledge, AcknowledgementHandler> onAck = mock(BiConsumer.class);
        when(eventHandlers.onAcknowledge()).thenReturn(onAck);
        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector<RestPayload> collector = createCollector();
        collector.listenForResponses();

        AcknowledgementHandler ackHandler = mock(AcknowledgementHandler.class);
        //send payload response
        collector.handleMessage(responseMessage, ackHandler);
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

        Collector<RestPayload> collector = createCollector();
        collector.listenForResponses();

        Acknowledge ack = new Acknowledge.Builder().withResponderId(Utils.generateId()).withResponsesRemaining(0).withTimeoutMs(timeoutMs).build();
        Message responseMessageWithAck = TestUtils.createMsbResponseMessageWithAckNoPayload(ack, TOPIC_RESPONSE, originalMessage.getCorrelationId());

        AcknowledgementHandler ackHandler = mock(AcknowledgementHandler.class);
        collector.handleMessage(responseMessageWithAck, ackHandler);

        verify(timeoutManagerMock, never()).enableResponseTimeout(eq(timeoutMs), eq(collector));
        verify(onEnd).call(any());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseReceivedAckWithUpdatedTimeoutAndNoResponsesRemaining() {
         /*ackTimeout = 0, responseTimeout= 100; waitForResponses = 0
        */
        int timeoutMs = 50;
        int timeoutMsInAck = 100;
        when(requestOptionsMock.getAckTimeout()).thenReturn(0);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(timeoutMs);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(0);

        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector<RestPayload> collector = createCollector();
        collector.listenForResponses();

        Acknowledge ack = new Acknowledge.Builder().withResponderId(Utils.generateId()).withResponsesRemaining(0).withTimeoutMs(timeoutMsInAck)
                .build();
        Message responseMessageWithAck = TestUtils.createMsbResponseMessageWithAckNoPayload(ack, TOPIC_RESPONSE, originalMessage.getCorrelationId());

        AcknowledgementHandler ackHandler = mock(AcknowledgementHandler.class);
        collector.handleMessage(responseMessageWithAck, ackHandler);

        verify(timeoutManagerMock, never()).enableResponseTimeout(eq(timeoutMsInAck), eq(collector));
        verify(onEnd).call(any());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseReceivedAckWithUpdatedTimeoutAndResponsesRemaining() {
        /*ackTimeout = 0, responseTimeout= 100; waitForResponses = 2
        */
        int timeoutMs = 100;
        int timeoutMsInAck = 200;
        int responsesRemaining = 2;
        when(requestOptionsMock.getAckTimeout()).thenReturn(0);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(timeoutMs);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(0);
        ArgumentCaptor<Integer> timeoutCaptor = ArgumentCaptor.forClass(Integer.class);

        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector<RestPayload> collector = createCollector();
        collector.listenForResponses();

        Acknowledge ack = new Acknowledge.Builder().withResponderId(Utils.generateId()).withResponsesRemaining(responsesRemaining)
                .withTimeoutMs(timeoutMsInAck).build();
        Message responseMessageWithAck = TestUtils.createMsbResponseMessageWithAckNoPayload(ack, TOPIC_RESPONSE, originalMessage.getCorrelationId());

        
        collector.handleMessage(responseMessageWithAck, null);

        verify(timeoutManagerMock).enableResponseTimeout(timeoutCaptor.capture(), any());

        assertThat(timeoutCaptor.getValue()).isBetween(1, timeoutMsInAck);
        verify(onEnd, never()).call(any());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseReceivedAcksWithUpdatedTimeoutAndResponsesRemaining() {
        /*ackTimeout = 0, responseTimeout= 50; waitForResponses = 2
        */
        int timeoutMs = 50;
        int timeoutMsInAckResponderOne = 2000;
        int responsesRemainingResponderOne = 5;
        int timeoutMsInAckResponderTwo = 5000;
        int responsesRemainingResponderTwo = 7;
        when(requestOptionsMock.getAckTimeout()).thenReturn(0);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(timeoutMs);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(0);

        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);
        ArgumentCaptor<Integer> timeoutCaptor = ArgumentCaptor.forClass(Integer.class);

        Collector<RestPayload> collector = createCollector();
        collector.listenForResponses();

        Acknowledge ackRespOne = new Acknowledge.Builder().withResponderId(Utils.generateId()).withResponsesRemaining(responsesRemainingResponderOne)
                .withTimeoutMs(timeoutMsInAckResponderOne).build();
        Message messageWithAckOne = TestUtils
                .createMsbResponseMessageWithAckNoPayload(ackRespOne, TOPIC_RESPONSE, originalMessage.getCorrelationId());

        Acknowledge ackRespTwo = new Acknowledge.Builder().withResponderId(Utils.generateId()).withResponsesRemaining(responsesRemainingResponderTwo)
                .withTimeoutMs(timeoutMsInAckResponderTwo).build();
        Message messageWithAckTwo = TestUtils
                .createMsbResponseMessageWithAckNoPayload(ackRespTwo, TOPIC_RESPONSE, originalMessage.getCorrelationId());

        collector.handleMessage(messageWithAckOne, null);
        verify(timeoutManagerMock).enableResponseTimeout(timeoutCaptor.capture(), any());
        assertEquals(responsesRemainingResponderOne, collector.getResponsesRemaining());
        assertThat(timeoutCaptor.getValue()).isBetween(1, timeoutMsInAckResponderOne);
        verify(onEnd, never()).call(any());

        collector.handleMessage(messageWithAckTwo, null);
        verify(timeoutManagerMock, times(2)).enableResponseTimeout(timeoutCaptor.capture(), any());
        assertEquals(responsesRemainingResponderOne + responsesRemainingResponderTwo, collector.getResponsesRemaining());
        assertThat(timeoutCaptor.getValue()).isBetween(1, timeoutMsInAckResponderTwo - 1);
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

        BiConsumer<RestPayload, AcknowledgementHandler> onResponse = mock(BiConsumer.class);
        when(eventHandlers.onResponse()).thenReturn(onResponse);
        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector<RestPayload> collector = createCollector();
        collector.listenForResponses();

        assertEquals(responsesRemaining, collector.getResponsesRemaining());

        //send first response
        collector.handleMessage(responseMessage, null);
        assertEquals(1, collector.getResponsesRemaining());
        verify(onEnd, never()).call(any());

        //send last response
        collector.handleMessage(responseMessage, null);
        assertEquals(0, collector.getResponsesRemaining());
        verify(onEnd).call(any());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseReceivedAckWithUpdatedTimeoutAndOneResponseRemaining() throws InterruptedException, IOException {
        int timeoutMs = 200;
        int timeoutMsInAck = 5000;
        String responderId = "b";

        when(requestOptionsMock.getAckTimeout()).thenReturn(0);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(timeoutMs);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(0);

        this.msbContext = TestUtils.createMsbContextBuilder()
                .withMsbConfigurations(msbConfigurationsMock)
                .withMessageFactory(messageFactoryMock)
                .withChannelManager(channelManagerMock)
                .withClock(Clock.systemDefaultZone())
                .withTimeoutManager(new TimeoutManager(1))
                .withCollectorManagerFactory(collectorManagerFactoryMock)
                .build();

        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector<RestPayload> collector = createCollector();
        collector.listenForResponses();

        Acknowledge ack = new Acknowledge.Builder().withResponderId(responderId).withResponsesRemaining(1).withTimeoutMs(timeoutMsInAck)
                .build();
        Message responseMessageWithAck = TestUtils.createMsbResponseMessageWithAckNoPayload(ack, TOPIC_RESPONSE, originalMessage.getCorrelationId());
        collector.handleMessage(responseMessageWithAck, null);

        //simulate responder response
        Acknowledge responseAck = new Acknowledge.Builder().withResponderId(responderId).withResponsesRemaining(-1).build();
        ObjectMapper payloadMapper = TestUtils.createMessageMapper();
        JsonNode payloadNode = payloadMapper.readValue(String.format("{\"body\": \"%s\" }", "test response payload body"), JsonNode.class);

        Message responderMessage = TestUtils.createMsbResponseMessage(responseAck, payloadNode, TOPIC_RESPONSE, "someCorrelationId");

        //send message
        collector.handleMessage(responderMessage, null);

        verify(onEnd, timeout(1500)).call(any());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testHandleResponseReceivedAckWithUpdatedTimeoutAndTwoResponsesRemaining() throws InterruptedException, IOException {
        int timeoutMs = 200;
        int timeoutMsInAck = 5000;
        String responderId = "b";

        when(requestOptionsMock.getAckTimeout()).thenReturn(0);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(timeoutMs);
        when(requestOptionsMock.getWaitForResponses()).thenReturn(0);

        this.msbContext = TestUtils.createMsbContextBuilder()
                .withMsbConfigurations(msbConfigurationsMock)
                .withMessageFactory(messageFactoryMock)
                .withChannelManager(channelManagerMock)
                .withClock(Clock.systemDefaultZone())
                .withTimeoutManager(new TimeoutManager(1))
                .withCollectorManagerFactory(collectorManagerFactoryMock)
                .build();

        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);

        Collector<RestPayload> collector = createCollector();
        collector.listenForResponses();

        Acknowledge ack = new Acknowledge.Builder().withResponderId(responderId).withResponsesRemaining(2).withTimeoutMs(timeoutMsInAck)
                .build();
        Message responseMessageWithAck = TestUtils.createMsbResponseMessageWithAckNoPayload(ack, TOPIC_RESPONSE, originalMessage.getCorrelationId());
        collector.handleMessage(responseMessageWithAck, null);

        //simulate responder response
        Acknowledge responseAck = new Acknowledge.Builder().withResponderId(responderId).withResponsesRemaining(-1).build();
        ObjectMapper payloadMapper = TestUtils.createMessageMapper();
        JsonNode payloadNode = payloadMapper.readValue(String.format("{\"body\": \"%s\" }", "test response payload body"), JsonNode.class);

        Message responderMessage = TestUtils.createMsbResponseMessage(responseAck, payloadNode, TOPIC_RESPONSE,  "someCorrelationId");

        //send first message
        collector.handleMessage(responderMessage, null);

        //send second message after initial response
        Thread.sleep(200);
        collector.handleMessage(responderMessage, null);

        verify(onEnd, timeout(1500)).call(any());
    }

    @Test
    public void testEndUnregisterCollector() {
        Collector<RestPayload> collector = createCollector();

        collector.end();

        verify(collectorManagerMock).unregisterCollector(collector);
    }

    @Test
    public void testEndHandlerEndCalled() {
        Callback<Void> onEnd = mock(Callback.class);
        when(eventHandlers.onEnd()).thenReturn(onEnd);
        Collector<RestPayload> collector = createCollector();

        collector.end();

        verify(onEnd).call(any());
    }

    @Test
    public void testProcessAckNull() {
        Collector<RestPayload> collector = createCollector();

        collector.processAck(null);

        verify(timeoutManagerMock, never()).enableResponseTimeout(anyInt(), any());
    }

    @Test
    public void testProcessAckPerResponder() {
        int timeoutMs = 5000;
        ArgumentCaptor<Integer> timeoutCaptor = ArgumentCaptor.forClass(Integer.class);
        Collector<RestPayload> collector = createCollector();

        collector.processAck(new Acknowledge.Builder().withResponderId("a").withTimeoutMs(timeoutMs).build());

        verify(timeoutManagerMock).enableResponseTimeout(timeoutCaptor.capture(), any());
        assertThat(timeoutCaptor.getValue()).isBetween(1, timeoutMs);
    }

    @Test
    public void testProcessAckWillTakeDefaultTimeoutIsMaxAndNotCallTimerAgain() {
        int timeoutMs = 1500;
        //will set default 3000;
        when(requestOptionsMock.getResponseTimeout()).thenReturn(null);
        Collector<RestPayload> collector = createCollector();

        collector.processAck(new Acknowledge.Builder().withResponderId("a").withTimeoutMs(timeoutMs).build());

        verify(timeoutManagerMock, never()).enableResponseTimeout(anyInt(), any());
    }

    @Test
    public void testEndHandlerTimersStopped() {
        ScheduledFuture ackTimerMock = mock(ScheduledFuture.class);
        ScheduledFuture timeoutTimerMock = mock(ScheduledFuture.class);
        when(timeoutManagerMock.enableAckTimeout(anyInt(), any())).thenReturn(ackTimerMock);
        when(timeoutManagerMock.enableResponseTimeout(anyInt(), any())).thenReturn(timeoutTimerMock);
        when(requestOptionsMock.getAckTimeout()).thenReturn(100);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(100);

        Collector<RestPayload> collector = createCollector();
        collector.listenForResponses();
        collector.waitForAcks();
        collector.waitForResponses();

        verify(timeoutManagerMock).enableAckTimeout(anyInt(), any());
        verify(timeoutManagerMock).enableResponseTimeout(anyInt(), any());

        collector.end();

        verify(ackTimerMock).cancel(anyBoolean());
        verify(timeoutTimerMock).cancel(anyBoolean());
    }

    @Test
    public void testListenForResponses() {
        Collector<RestPayload> collector = createCollector();
        collector.listenForResponses();

        verify(collectorManagerMock).registerCollector(collector);
    }

    @Test
    public void testWaitForResponses() throws InterruptedException {
        int timeoutMs = 1000;
        int initCollectorAfter = 50;
        ArgumentCaptor<Integer> timeoutCaptor = ArgumentCaptor.forClass(Integer.class);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(timeoutMs);
        Collector<RestPayload> collector = createCollector();
        Thread.sleep(initCollectorAfter);
        collector.waitForResponses();

        verify(timeoutManagerMock).enableResponseTimeout(timeoutCaptor.capture(), any());
        int timeoutLeftToWait = timeoutMs - initCollectorAfter;
        assertThat(timeoutCaptor.getValue()).isBetween(1, timeoutLeftToWait);
    }

    @Test
    public void testWaitForResponsesReceivedGreaterTimeout() throws InterruptedException {
        int timeoutMs = 1000;
        int updatedTimeoutMs = 2000;
        int receivedAckAfter = 50;
        ArgumentCaptor<Integer> timeoutCaptor = ArgumentCaptor.forClass(Integer.class);
        when(requestOptionsMock.getResponseTimeout()).thenReturn(timeoutMs);
        Collector<RestPayload> collector = createCollector();
        Thread.sleep(receivedAckAfter);
        collector.processAck(new Acknowledge.Builder().withResponderId("a").withTimeoutMs(updatedTimeoutMs).build());
        collector.waitForResponses();

        verify(timeoutManagerMock, times(2)).enableResponseTimeout(timeoutCaptor.capture(), any());
        int timeoutLeftToWait = updatedTimeoutMs - receivedAckAfter;
        assertThat(timeoutCaptor.getValue()).isBetween(1, timeoutLeftToWait);
    }

    @Test
    public void testWaitForAcks() {
        int timeoutMs = 1000;
        ArgumentCaptor<Integer> timeoutCaptor = ArgumentCaptor.forClass(Integer.class);
        when(requestOptionsMock.getAckTimeout()).thenReturn(timeoutMs);
        Collector<RestPayload> collector = createCollector();

        //set waitForAcksUntil value
        collector.listenForResponses();
        collector.waitForAcks();

        verify(timeoutManagerMock).enableAckTimeout(timeoutCaptor.capture(), any());
        assertThat(timeoutCaptor.getValue()).isBetween(500, timeoutMs);
    }

    private Collector<RestPayload> createCollector() {
        return new Collector<>(TOPIC, originalMessage, requestOptionsMock, msbContext, eventHandlers, new TypeReference<RestPayload>() {
        });
    }

}