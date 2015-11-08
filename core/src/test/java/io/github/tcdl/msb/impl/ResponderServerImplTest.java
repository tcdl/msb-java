package io.github.tcdl.msb.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.Producer;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.Responder;
import io.github.tcdl.msb.api.ResponderServer;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.RestPayload;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.anyString;
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
        ResponderServer.RequestHandler<String> handler = (request, responder) -> {
        };

        ArgumentCaptor<MessageHandler> subscriberCaptor = ArgumentCaptor.forClass(MessageHandler.class);
        ChannelManager spyChannelManager = spy(msbContext.getChannelManager());
        MsbContextImpl spyMsbContext = spy(msbContext);

        when(spyMsbContext.getChannelManager()).thenReturn(spyChannelManager);

        ResponderServerImpl<String> responderServer = ResponderServerImpl
                .create(TOPIC, requestOptions.getMessageTemplate(), spyMsbContext, handler, new TypeReference<String>() {});

        ResponderServerImpl spyResponderServer = (ResponderServerImpl) spy(responderServer).listen();

        verify(spyChannelManager).subscribe(anyString(), subscriberCaptor.capture());

        Message originalMessage = TestUtils.createSimpleRequestMessage(TOPIC);
        subscriberCaptor.getValue().handleMessage(originalMessage);

        verify(spyResponderServer).onResponder(anyObject());
    }

    @Test(expected = NullPointerException.class)
    public void testResponderServerProcessErrorNoHandler() throws Exception {
        msbContext.getObjectFactory().createResponderServer(TOPIC, messageTemplate, null);
    }

    @Test
    public void testResponderServerProcessUnexpectedPayload() throws Exception {
        ResponderServer.RequestHandler<Integer> handler = (request, responder) -> {
        };

        String bodyText = "some body";
        Message incomingMessage = TestUtils.createMsbRequestMessage(TOPIC, bodyText);

        ResponderServerImpl<Integer> responderServer = ResponderServerImpl
                .create(TOPIC, messageTemplate, msbContext, handler, new TypeReference<Integer>() {});
        responderServer.listen();

        // simulate incoming request
        ArgumentCaptor<RestPayload> responseCaptor = ArgumentCaptor.forClass(RestPayload.class);
        ResponderImpl responder = spy(
                new ResponderImpl(messageTemplate, incomingMessage, msbContext));
        responderServer.onResponder(responder);
        verify(responder).send(responseCaptor.capture());
        assertEquals(ResponderServer.PAYLOAD_CONVERSION_ERROR_CODE, responseCaptor.getValue().getStatusCode().intValue());
        assertNotNull(responseCaptor.getValue().getStatusMessage());
    }

    @Test
    public void testResponderServerProcessHandlerThrowException() throws Exception {
        String exceptionMessage = "Test exception message";
        Exception error = new Exception(exceptionMessage);
        ResponderServer.RequestHandler<String> handler = (request, responder) -> {
            throw error;
        };

        ResponderServerImpl<String> responderServer = ResponderServerImpl
                .create(TOPIC, messageTemplate, msbContext, handler, new TypeReference<String>() {});
        responderServer.listen();

        // simulate incoming request
        ArgumentCaptor<RestPayload> responseCaptor = ArgumentCaptor.forClass(RestPayload.class);
        ResponderImpl responder = spy(
                new ResponderImpl(messageTemplate, TestUtils.createMsbRequestMessageNoPayload(TOPIC), msbContext));
        responderServer.onResponder(responder);

        verify(responder).send(responseCaptor.capture());
        assertEquals(ResponderServer.INTERNAL_SERVER_ERROR_CODE, responseCaptor.getValue().getStatusCode().intValue());
        assertEquals(exceptionMessage, responseCaptor.getValue().getStatusMessage());
    }

    @Test
    public void testCreateResponderWithResponseTopic() {
        ResponderServer.RequestHandler<String> handler = (request, responder) -> {
        };

        ChannelManager mockChannelManager = mock(ChannelManager.class);
        Producer mockProducer = mock(Producer.class);
        when(mockChannelManager.findOrCreateProducer(anyString())).thenReturn(mockProducer);
        MsbContextImpl msbContext1 = new TestUtils.TestMsbContextBuilder()
                .withChannelManager(mockChannelManager)
                .build();

        ResponderServerImpl<String> responderServer = ResponderServerImpl
                .create(TOPIC, messageTemplate, msbContext1, handler, new TypeReference<String>() {});

        Message incomingMessage = TestUtils.createMsbRequestMessageNoPayload(TOPIC);
        Responder responder = responderServer.createResponder(incomingMessage);
        assertEquals(incomingMessage, responder.getOriginalMessage());

        responder.sendAck(1, 1);
        responder.send("response");

        // Verify that 2 messages were published
        verify(mockProducer, times(2)).publish(any(Message.class));
    }

    @Test
    public void testCreateResponderNoResponseTopic() {
        ResponderServer.RequestHandler<String> handler = (request, responder) -> {
        };

        ChannelManager mockChannelManager = mock(ChannelManager.class);
        MsbContextImpl msbContext = new TestUtils.TestMsbContextBuilder()
                .withChannelManager(mockChannelManager)
                .build();

        ResponderServerImpl<String> responderServer = ResponderServerImpl
                .create(TOPIC, messageTemplate, msbContext, handler, new TypeReference<String>() {});

        Message incomingMessage = TestUtils.createMsbBroadcastMessageNoPayload(TOPIC);
        Responder responder = responderServer.createResponder(incomingMessage);
        assertEquals(incomingMessage, responder.getOriginalMessage());

        responder.sendAck(1, 1);
        responder.send("response");

        // Verify that no messages were published
        verifyZeroInteractions(mockChannelManager);
    }
}
