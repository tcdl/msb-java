package io.github.tcdl.msb.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.ResponderServer;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.message.payload.MyPayload;
import io.github.tcdl.msb.support.TestUtils;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

/**
 * Created by rdro on 4/30/2015.
 */
public class ResponderServerImplTest {

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
    public void testResponderServerProcessCustomPayloadSuccess() throws Exception {
        String namespace = TestUtils.getSimpleNamespace();
        ResponderServer.RequestHandler<MyPayload> handler = (request, responder) -> {
        };

        ArgumentCaptor<MessageHandler> subscriberCaptor = ArgumentCaptor.forClass(MessageHandler.class);
        ChannelManager spyChannelManager = spy(msbContext.getChannelManager());
        MsbContextImpl spyMsbContext = spy(msbContext);

        when(spyMsbContext.getChannelManager()).thenReturn(spyChannelManager);

        ResponderServerImpl responderServer = ResponderServerImpl
                .create(namespace, requestOptions.getMessageTemplate(), spyMsbContext, handler, new TypeReference<MyPayload>() {});

        ResponderServerImpl spyResponderServer = (ResponderServerImpl) spy(responderServer).listen();

        verify(spyChannelManager).subscribe(anyString(), subscriberCaptor.capture());

        Message originalMessage = TestUtils.createMsbRequestMessageWithSimplePayload(namespace);
        subscriberCaptor.getValue().handleMessage(originalMessage);

        verify(spyResponderServer).onResponder(anyObject());
    }

    @Test
    public void testResponderServerProcessDefaultPayloadSuccess() throws Exception {
        String namespace = TestUtils.getSimpleNamespace();
        ResponderServer.RequestHandler<Payload> handler = (request, responder) -> {
        };

        ArgumentCaptor<MessageHandler> subscriberCaptor = ArgumentCaptor.forClass(MessageHandler.class);
        ChannelManager spyChannelManager = spy(msbContext.getChannelManager());
        MsbContextImpl spyMsbContext = spy(msbContext);

        when(spyMsbContext.getChannelManager()).thenReturn(spyChannelManager);

        ResponderServerImpl responderServer = ResponderServerImpl
                .create(namespace, requestOptions.getMessageTemplate(), spyMsbContext, handler, new TypeReference<Payload>() {});

        ResponderServerImpl spyResponderServer = (ResponderServerImpl) spy(responderServer).listen();

        verify(spyChannelManager).subscribe(anyString(), subscriberCaptor.capture());

        Message originalMessage = TestUtils.createMsbRequestMessageWithSimplePayload(namespace);
        subscriberCaptor.getValue().handleMessage(originalMessage);

        verify(spyResponderServer).onResponder(anyObject());
    }

    @Test(expected = NullPointerException.class)
    public void testResponderServerProcessErrorNoHandler() throws Exception {
        msbContext.getObjectFactory().createResponderServer(TestUtils.getSimpleNamespace(), messageTemplate, null);
    }

    @Test
    public void testResponderServerProcessWithError() throws Exception {
        Exception error = new Exception();
        ResponderServer.RequestHandler<MyPayload> handler = (request, responder) -> {
            throw error;
        };

        ResponderServerImpl responderServer = ResponderServerImpl
                .create(TestUtils.getSimpleNamespace(), messageTemplate, msbContext, handler, new TypeReference<MyPayload>() {});
        responderServer.listen();

        // simulate incoming request
        ArgumentCaptor<Payload> responseCaptor = ArgumentCaptor.forClass(Payload.class);
        ResponderImpl responder = spy(
                new ResponderImpl(messageTemplate, TestUtils.createMsbRequestMessageNoPayload(TestUtils.getSimpleNamespace()), msbContext));
        responderServer.onResponder(responder);

        verify(responder).send(responseCaptor.capture());
        assertEquals(500, responseCaptor.getValue().getStatusCode().intValue());
    }
}