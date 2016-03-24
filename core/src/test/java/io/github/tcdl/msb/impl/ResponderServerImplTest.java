package io.github.tcdl.msb.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.Producer;
import io.github.tcdl.msb.api.AcknowledgementHandler;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.Responder;
import io.github.tcdl.msb.api.ResponderContext;
import io.github.tcdl.msb.api.ResponderServer;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.RestPayload;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

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
        ResponderServer.RequestHandler<RestPayload<Object, Map<String, String>, Object, Map<String, String>>> 
            handler = (request, responderContext) -> {
            };

        ArgumentCaptor<MessageHandler> subscriberCaptor = ArgumentCaptor.forClass(MessageHandler.class);
        ChannelManager spyChannelManager = spy(msbContext.getChannelManager());
        MsbContextImpl spyMsbContext = spy(msbContext);

        when(spyMsbContext.getChannelManager()).thenReturn(spyChannelManager);

        ResponderServerImpl<RestPayload<Object, Map<String, String>, Object, Map<String, String>>> responderServer = ResponderServerImpl
                .create(TOPIC, requestOptions.getMessageTemplate(), spyMsbContext, handler, null,
                        new TypeReference<RestPayload<Object, Map<String, String>, Object, Map<String, String>>>() {});

        ResponderServerImpl spyResponderServer = (ResponderServerImpl) spy(responderServer).listen();

        verify(spyChannelManager).subscribe(anyString(), subscriberCaptor.capture());

        Message originalMessage = TestUtils.createSimpleRequestMessage(TOPIC);
        AcknowledgementHandler mockAcknowledgeHandler = mock(AcknowledgementHandler.class);
        
        subscriberCaptor.getValue().handleMessage(originalMessage, null);

        verify(spyResponderServer).onResponder(anyObject());
    }

    @Test(expected = NullPointerException.class)
    public void testResponderServerProcessErrorNoHandler() throws Exception {
        msbContext.getObjectFactory().createResponderServer(TOPIC, messageTemplate, null);
    }

    @Test
    public void testResponderServerProcessUnexpectedPayload() throws Exception {
        ResponderServer.RequestHandler<Integer> handler = (request, responderContext) -> {
        };

        String bodyText = "some body";
        Message incomingMessage = TestUtils.createMsbRequestMessage(TOPIC, bodyText);

        ResponderServerImpl<Integer> responderServer = ResponderServerImpl
                .create(TOPIC, messageTemplate, msbContext, handler, null, new TypeReference<Integer>() {});
        responderServer.listen();

        // simulate incoming request
        ArgumentCaptor<RestPayload> responseCaptor = ArgumentCaptor.forClass(RestPayload.class);
        ResponderImpl responder = spy(
                new ResponderImpl(messageTemplate, incomingMessage, msbContext));
        
        AcknowledgementHandler acknowledgeHandler = mock(AcknowledgementHandler.class);
        ResponderContext responderContext = responderServer.createResponderContext(responder, acknowledgeHandler, incomingMessage);
        
        responderServer.onResponder(responderContext);
        verify(responder).send(responseCaptor.capture());
        assertEquals(ResponderServer.PAYLOAD_CONVERSION_ERROR_CODE, responseCaptor.getValue().getStatusCode().intValue());
        assertNotNull(responseCaptor.getValue().getStatusMessage());
    }

    @Test
    public void testResponderServerProcessHandlerThrowException() throws Exception {
        String exceptionMessage = "Test exception message";
        Exception error = new Exception(exceptionMessage);
        ResponderServer.RequestHandler<String> handler = (request, responderContext) -> {
            throw error;
        };

        ResponderServerImpl<String> responderServer = ResponderServerImpl
                .create(TOPIC, messageTemplate, msbContext, handler, null, new TypeReference<String>() {});
        responderServer.listen();

        // simulate incoming request
        ArgumentCaptor<RestPayload> responseCaptor = ArgumentCaptor.forClass(RestPayload.class);
        Message originalMessage = TestUtils.createMsbRequestMessageNoPayload(TOPIC);
        ResponderImpl responder = spy(
                new ResponderImpl(messageTemplate, originalMessage, msbContext));
        AcknowledgementHandler acknowledgeHandler = mock(AcknowledgementHandler.class);
        ResponderContext responderContext = responderServer.createResponderContext(responder, acknowledgeHandler, originalMessage);
        
        responderServer.onResponder(responderContext);

        verify(responder).send(responseCaptor.capture());
        verify(acknowledgeHandler).confirmMessage();
        
        assertEquals(ResponderServer.INTERNAL_SERVER_ERROR_CODE, responseCaptor.getValue().getStatusCode().intValue());
        assertEquals(exceptionMessage, responseCaptor.getValue().getStatusMessage());
    }

    @Test
    public void testCreateResponderWithResponseTopic() {
        ResponderServer.RequestHandler<String> handler = (request, responderContext) -> {
        };

        ChannelManager mockChannelManager = mock(ChannelManager.class);
        Producer mockProducer = mock(Producer.class);
        when(mockChannelManager.findOrCreateProducer(anyString())).thenReturn(mockProducer);
        MsbContextImpl msbContext1 = new TestUtils.TestMsbContextBuilder()
                .withChannelManager(mockChannelManager)
                .build();

        ResponderServerImpl<String> responderServer = ResponderServerImpl
                .create(TOPIC, messageTemplate, msbContext1, handler, null, new TypeReference<String>() {});

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
    public void testCreateResponderNoResponseTopic() {
        ResponderServer.RequestHandler<String> handler = (request, responderContext) -> {
        };

        ChannelManager mockChannelManager = mock(ChannelManager.class);
        MsbContextImpl msbContext = new TestUtils.TestMsbContextBuilder()
                .withChannelManager(mockChannelManager)
                .build();

        ResponderServerImpl<String> responderServer = ResponderServerImpl
                .create(TOPIC, messageTemplate, msbContext, handler, null, new TypeReference<String>() {});

        Message incomingMessage = TestUtils.createMsbBroadcastMessageNoPayload(TOPIC);
        Responder responder = responderServer.createResponder(incomingMessage);

        responder.sendAck(1, 1);
        responder.send("response");

        // Verify that no messages were published
        verifyZeroInteractions(mockChannelManager);
    }
}
