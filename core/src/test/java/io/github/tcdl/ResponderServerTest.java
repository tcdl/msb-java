package io.github.tcdl;

import io.github.tcdl.config.MessageTemplate;
import io.github.tcdl.config.RequestOptions;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by rdro on 4/30/2015.
 */
public class ResponderServerTest {

    private RequestOptions requestOptions;
    private MessageTemplate messageTemplate;

    private MsbContext msbContext = TestUtils.createSimpleMsbContext();

    @Before
    public void setUp() {
        requestOptions = TestUtils.createSimpleRequestOptions();
        messageTemplate = TestUtils.createSimpleMessageTemplate();
        msbContext = TestUtils.createSimpleMsbContext();
    }

    @Test
    public void testResponderServerProcessSuccess() throws Exception {

        String namespace = TestUtils.getSimpleNamespace();
        ResponderServer.RequestHandler handler = (request, responder) -> {
        };

        ArgumentCaptor<Consumer.Subscriber> subscriberCaptor = ArgumentCaptor.forClass(Consumer.Subscriber.class);
        ChannelManager spyChannelManager = spy(msbContext.getChannelManager());
        MsbContext spyMsbContext = spy(msbContext);

        when(spyMsbContext.getChannelManager()).thenReturn(spyChannelManager);

        ResponderServer responderServer = ResponderServer
                .create(namespace, requestOptions.getMessageTemplate(), spyMsbContext, handler);

        ResponderServer spyResponderServer = spy(responderServer).listen();

        verify(spyChannelManager).subscribe(anyString(), subscriberCaptor.capture());

        Message originalMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(namespace);
        subscriberCaptor.getValue().handleMessage(originalMessage);

        verify(spyResponderServer).onResponder(anyObject());
    }

    @Test(expected = NullPointerException.class)
    public void testResponderServerProcessErrorNoHandler() throws Exception {
        ResponderServer.create(TestUtils.getSimpleNamespace(), messageTemplate, msbContext, null);
    }

    @Test
    public void testResponderServerProcessWithError() throws Exception {

        Exception error = new Exception();
        ResponderServer.RequestHandler handler = (request, responder) -> { throw error; };

        ResponderServer responderServer = ResponderServer
                .create(TestUtils.getSimpleNamespace(), messageTemplate, msbContext, handler)
                .listen();

        // simulate incoming request
        ArgumentCaptor<Payload> responseCaptor = ArgumentCaptor.forClass(Payload.class);
        Responder responder = spy(new Responder(messageTemplate, TestUtils.createMsbRequestMessageNoPayload(TestUtils.getSimpleNamespace()), msbContext));
        responderServer.onResponder(responder);

        verify(responder).send(responseCaptor.capture());
        assertEquals(500, responseCaptor.getValue().getStatusCode().intValue());
    }
}
