package io.github.tcdl;

import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Created by rdro on 4/30/2015.
 */
public class ResponderServerTest {
    private MsbMessageOptions messageOptions;

    private MsbContext msbContext = TestUtils.createSimpleMsbContext();

    @Before
    public void setUp() {
        messageOptions = TestUtils.createSimpleConfig();
        msbContext = TestUtils.createSimpleMsbContext();
    }

    @Test
    public void testResponderServerProcessSuccess() throws Exception {

        ResponderServer.RequestHandler handler = (request, responder) -> {
        };

        ArgumentCaptor<Consumer.Subscriber> subscriberCaptor = ArgumentCaptor.forClass(Consumer.Subscriber.class);
        ChannelManager spyChannelManager = spy(msbContext.getChannelManager());
        MsbContext spyMsbContext = spy(msbContext);

        when(spyMsbContext.getChannelManager()).thenReturn(spyChannelManager);

        ResponderServer responderServer = ResponderServer
                .create(messageOptions, spyMsbContext, handler);

        ResponderServer spyResponderServer = spy(responderServer).listen();

        verify(spyChannelManager).subscribe(anyString(), subscriberCaptor.capture());

        Message originalMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(messageOptions.getNamespace());
        subscriberCaptor.getValue().handleMessage(originalMessage);

        verify(spyResponderServer).onResponder(anyObject());
    }

    @Test(expected = NullPointerException.class)
    public void testResponderServerProcessErrorNoHandler() throws Exception {
        ResponderServer.create(messageOptions, msbContext, null);
    }

    @Test
    public void testResponderServerProcessWithError() throws Exception {

        Exception error = new Exception();
        ResponderServer.RequestHandler handler = (request, responder) -> { throw error; };

        ResponderServer responderServer = ResponderServer
                .create(messageOptions, msbContext, handler)
                .listen();

        // simulate incoming request
        ArgumentCaptor<Payload> responseCaptor = ArgumentCaptor.forClass(Payload.class);
        Responder responder = spy(new Responder(messageOptions, TestUtils.createMsbRequestMessageNoPayload(), msbContext));
        responderServer.onResponder(responder);

        verify(responder).send(responseCaptor.capture());
        assertEquals(500, responseCaptor.getValue().getStatusCode().intValue());
    }
}
