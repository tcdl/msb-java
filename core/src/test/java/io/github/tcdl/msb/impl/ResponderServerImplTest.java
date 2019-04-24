package io.github.tcdl.msb.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Sets;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.Producer;
import io.github.tcdl.msb.api.*;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.RestPayload;
import io.github.tcdl.msb.api.metrics.Gauge;
import io.github.tcdl.msb.api.metrics.MetricSet;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.*;

public class ResponderServerImplTest {

    private static final String TOPIC = "test:responder-server";

    private RequestOptions requestOptions;
    private MessageTemplate messageTemplate;

    private MsbContextImpl msbContext = TestUtils.createSimpleMsbContext();

    @Before
    public void setUp() {
        requestOptions = TestUtils.createSimpleRequestOptions();
        messageTemplate = TestUtils.createSimpleMessageTemplate();
        msbContext = TestUtils.createSimpleMsbContext();
    }

    @Test
    public void testResponderServerProcessPayloadSuccess() throws Exception {
        Message originalMessage = TestUtils.createSimpleRequestMessage(TOPIC);

        ResponderServer.RequestHandler<RestPayload<Object, Map<String, String>, Object, Map<String, String>>> handler =
                (request, responderContext) -> {
                    assertEquals("MessageContext must contain original message during message handler execution",
                            originalMessage, MsbThreadContext.getMessageContext().getOriginalMessage());
                    assertEquals("MsbThreadContext must contain a Request during message handler execution",
                            request, MsbThreadContext.getRequest());
                };

        ArgumentCaptor<MessageHandler> subscriberCaptor = ArgumentCaptor.forClass(MessageHandler.class);
        ChannelManager spyChannelManager = spy(msbContext.getChannelManager());
        MsbContextImpl spyMsbContext = spy(msbContext);

        when(spyMsbContext.getChannelManager()).thenReturn(spyChannelManager);

        ResponderOptions responderOptions = new ResponderOptions.Builder().withBindingKeys(Collections.emptySet()).withMessageTemplate(messageTemplate).build();

        ResponderServerImpl<RestPayload<Object, Map<String, String>, Object, Map<String, String>>> responderServer =
                ResponderServerImpl.create(TOPIC,responderOptions, spyMsbContext, handler, null,
                        new TypeReference<RestPayload<Object, Map<String, String>, Object, Map<String, String>>>() {});

        ResponderServerImpl spyResponderServer = (ResponderServerImpl) spy(responderServer).listen();

        verify(spyChannelManager).subscribe(anyString(), any(ResponderOptions.class), subscriberCaptor.capture());

        assertNull("MessageContext must be absent outside message handler execution", MsbThreadContext.getMessageContext());
        assertNull("Request must be absent outside message handler execution", MsbThreadContext.getRequest());
        subscriberCaptor.getValue().handleMessage(originalMessage, null);
        assertNull("MessageContext must be absent outside message handler execution", MsbThreadContext.getMessageContext());
        assertNull("Request must be absent outside message handler execution", MsbThreadContext.getRequest());

        verify(spyResponderServer).onResponder(anyObject());
    }

    @Test
    public void testResponderServerMetrics() {
        ResponderServer.RequestHandler<RestPayload<Object, Map<String, String>, Object, Map<String, String>>> handler =
                (request, responderContext) -> {};
        ResponderOptions responderOptions = new ResponderOptions.Builder().withBindingKeys(Collections.emptySet()).withMessageTemplate(messageTemplate).build();
        MsbContextImpl spyMsbContext = spy(msbContext);

        ChannelManager spyChannelManager = spy(msbContext.getChannelManager());
        when(spyMsbContext.getChannelManager()).thenReturn(spyChannelManager);
        when(spyChannelManager.getAvailableMessageCount(anyString())).thenReturn(Optional.of(666L));
        when(spyChannelManager.isConnected(anyString())).thenReturn(Optional.of(true));

        ResponderServer responderServer =
                ResponderServerImpl.create(TOPIC,responderOptions, spyMsbContext, handler, null,
                        new TypeReference<RestPayload<Object, Map<String, String>, Object, Map<String, String>>>() {})
                .listen();


        MetricSet metricSet = responderServer.getMetrics();
        Gauge<Long> availableMessageCount = (Gauge<Long>) metricSet.getMetric("availableMessageCount");
        Gauge<Boolean> isConsumerConnected = (Gauge<Boolean>) metricSet.getMetric("consumerConnected");

        assertEquals(666L, availableMessageCount.getValue().longValue());
        assertTrue(isConsumerConnected.getValue());

        verify(spyChannelManager, times(1)).subscribe(eq(TOPIC), any(ResponderOptions.class), any(MessageHandler.class));
        verify(spyChannelManager, times(1)).getAvailableMessageCount(TOPIC);
        verify(spyChannelManager, times(1)).isConnected(TOPIC);
    }

    @Test(expected = NullPointerException.class)
    public void testResponderServerProcessErrorNoHandler() throws Exception {
        msbContext.getObjectFactory().createResponderServer(TOPIC, messageTemplate, null);
    }

    @Test
    public void testResponderServerProcessUnexpectedPayload() throws Exception {
        ResponderServer.RequestHandler<Integer> handler = (request, responderContext) -> {};

        String bodyText = "some body";
        Message incomingMessage = TestUtils.createMsbRequestMessage(TOPIC, bodyText);

        ResponderOptions responderOptions = new ResponderOptions.Builder().withMessageTemplate(messageTemplate).build();

        ResponderServerImpl<Integer> responderServer = ResponderServerImpl
                .create(TOPIC, responderOptions, msbContext, handler, null, new TypeReference<Integer>() {});
        responderServer.listen();

        // simulate incoming request
        ResponderImpl responder = spy(new ResponderImpl(messageTemplate, incomingMessage, msbContext));

        AcknowledgementHandler acknowledgeHandler = mock(AcknowledgementHandler.class);
        ResponderContext responderContext = responderServer.createResponderContext(responder, acknowledgeHandler, incomingMessage);
        
        responderServer.onResponder(responderContext);
        verify(responder).sendAck(0, 0);
        verify(acknowledgeHandler).confirmMessage();
    }

    @Test
    public void testResponderServerProcessHandlerThrowException() throws Exception {
        String exceptionMessage = "Test exception message";
        Exception error = new Exception(exceptionMessage);
        ResponderServer.RequestHandler<String> handler = (request, responderContext) -> { throw error; };

        ResponderOptions responderOptions = new ResponderOptions.Builder().withMessageTemplate(messageTemplate).build();

        ResponderServerImpl<String> responderServer = ResponderServerImpl
                .create(TOPIC, responderOptions, msbContext, handler, null, new TypeReference<String>() {});
        responderServer.listen();

        // simulate incoming request
        Message originalMessage = TestUtils.createMsbRequestMessageNoPayload(TOPIC);
        ResponderImpl responder = spy(
                new ResponderImpl(messageTemplate, originalMessage, msbContext));
        AcknowledgementHandler acknowledgeHandler = mock(AcknowledgementHandler.class);
        ResponderContext responderContext = responderServer.createResponderContext(responder, acknowledgeHandler, originalMessage);

        responderServer.onResponder(responderContext);
        verify(responder).sendAck(0, 0);
        verify(acknowledgeHandler).confirmMessage();
    }

    @Test
    public void testResponderServerProcessCustomHandlerThrowException() throws Exception {
        String exceptionMessage = "Test exception message";
        Exception error = new Exception(exceptionMessage);
        ResponderServer.RequestHandler<String> handler = (request, responderContext) -> {
            throw error;
        };

        ResponderOptions responderOptions = new ResponderOptions.Builder().withMessageTemplate(messageTemplate).build();
        ResponderServer.ErrorHandler errorHandlerMock = mock(ResponderServer.ErrorHandler.class);
        ResponderServerImpl<String> responderServer = ResponderServerImpl
                .create(TOPIC, responderOptions, msbContext, handler, errorHandlerMock, new TypeReference<String>() {});
        responderServer.listen();

        // simulate incoming request
        Message originalMessage = TestUtils.createMsbRequestMessageNoPayload(TOPIC);
        Responder responder = mock(Responder.class);
        AcknowledgementHandler acknowledgeHandler = mock(AcknowledgementHandler.class);
        ResponderContext responderContext = responderServer.createResponderContext(responder, acknowledgeHandler, originalMessage);

        responderServer.onResponder(responderContext);

        verify(errorHandlerMock).handle(eq(error), eq(originalMessage));
    }

    @Test
    public void testCreateResponderWithResponseTopic() {
        ResponderServer.RequestHandler<String> handler = (request, responderContext) -> {
        };

        ChannelManager mockChannelManager = mock(ChannelManager.class);
        Producer mockProducer = mock(Producer.class);
        when(mockChannelManager.findOrCreateProducer(anyString(), eq(true), any(RequestOptions.class))).thenReturn(mockProducer);
        MsbContextImpl msbContext1 = new TestUtils.TestMsbContextBuilder()
                .withChannelManager(mockChannelManager)
                .build();

        ResponderOptions responderOptions = new ResponderOptions.Builder().withBindingKeys(Collections.emptySet()).withMessageTemplate(messageTemplate).build();
        ResponderServerImpl<String> responderServer = ResponderServerImpl
                .create(TOPIC, responderOptions, msbContext1, handler, null, new TypeReference<String>() {});

        Message incomingMessage = TestUtils.createMsbRequestMessageNoPayload(TOPIC);
        Responder responder = responderServer.createResponder(incomingMessage);
        ResponderContext responderContext = responderServer.createResponderContext(responder, null, incomingMessage);
        assertEquals(incomingMessage, responderContext.getOriginalMessage());

        responder.sendAck(1, 1);
        responder.send("response");

        // Verify that 2 messages were published
        verify(mockProducer, times(2)).publish(any(Message.class));
    }

    @Test
    public void testCreateResponderWithRoutingKeys() throws Exception {

        ChannelManager mockChannelManager = mock(ChannelManager.class);
        MsbContextImpl msbContext = new TestUtils.TestMsbContextBuilder()
                .withChannelManager(mockChannelManager)
                .build();

        Set<String> bindingKeys = Sets.newHashSet("routing.key.one", "routing.key.two");

        ResponderServer.RequestHandler<String> requestHandler = (request, responderContext) -> {};

        ResponderOptions responderOptions = new ResponderOptions.Builder().withBindingKeys(bindingKeys).withMessageTemplate(messageTemplate).build();
        ResponderServerImpl<String> responderServer = ResponderServerImpl
                .create(TOPIC, responderOptions, msbContext, requestHandler, null, new TypeReference<String>() {});

        responderServer.listen();
        verify(mockChannelManager).subscribe(eq(TOPIC), eq(responderOptions), any(MessageHandler.class));
    }

    @Test
    public void testCreateResponderWithoutRoutingKeys() throws Exception {

        ChannelManager mockChannelManager = mock(ChannelManager.class);
        MsbContextImpl msbContext = new TestUtils.TestMsbContextBuilder()
                .withChannelManager(mockChannelManager)
                .build();

        ResponderServer.RequestHandler<String> requestHandler = (request, responderContext) -> {};

        ResponderOptions responderOptions = new ResponderOptions.Builder().withBindingKeys(Collections.emptySet()).withMessageTemplate(messageTemplate).build();

        ResponderServerImpl<String> responderServer = ResponderServerImpl
                .create(TOPIC, responderOptions, msbContext, requestHandler, null, new TypeReference<String>() {});

        responderServer.listen();
        verify(mockChannelManager).subscribe(eq(TOPIC), same(responderOptions), any(MessageHandler.class));
    }

    @Test
    public void testCreateResponderNoResponseTopic() {
        ResponderServer.RequestHandler<String> handler = (request, responderContext) -> {
        };

        ChannelManager mockChannelManager = mock(ChannelManager.class);
        MsbContextImpl msbContext = new TestUtils.TestMsbContextBuilder()
                .withChannelManager(mockChannelManager)
                .build();

        ResponderOptions responderOptions = new ResponderOptions.Builder().withMessageTemplate(messageTemplate).build();

        ResponderServerImpl<String> responderServer = ResponderServerImpl
                .create(TOPIC, responderOptions, msbContext, handler, null, new TypeReference<String>() {});

        Message incomingMessage = TestUtils.createMsbBroadcastMessageNoPayload(TOPIC);
        Responder responder = responderServer.createResponder(incomingMessage);

        responder.sendAck(1, 1);
        responder.send("response");

        // Verify that no messages were published
        verifyZeroInteractions(mockChannelManager);
    }

    @Test
    public void testStop() throws Exception {
        ResponderServer.RequestHandler<String> doNothingHandler = (request, responderContext) -> {};

        ChannelManager mockChannelManager = mock(ChannelManager.class);
        MsbContextImpl msbContext = new TestUtils.TestMsbContextBuilder()
                .withChannelManager(mockChannelManager)
                .build();

        ResponderOptions responderOptions = new ResponderOptions.Builder().withMessageTemplate(messageTemplate).build();

        ResponderServerImpl<String> responderServer = ResponderServerImpl.create(
                TOPIC, responderOptions, msbContext, doNothingHandler, null, new TypeReference<String>() {}
        );

        responderServer.stop();
        verify(mockChannelManager).unsubscribe(TOPIC);
    }
}
