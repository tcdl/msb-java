package io.github.tcdl;

import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.middleware.Middleware;
import io.github.tcdl.support.TestUtils;
import io.github.tcdl.support.Utils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
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
    public void testResponderServerAsyncProcessSuccess() throws Exception {

        Middleware middleware = (request, responder) -> {
        };

        ChannelManager channelManager = msbContext.getChannelManager();
        Consumer consumer = channelManager.findOrCreateConsumer(messageOptions.getNamespace());

        ChannelManager spyChannelManager = spy(channelManager);
        MsbContext spyMsbContext = spy(msbContext);

        when(spyMsbContext.getChannelManager()).thenReturn(spyChannelManager);
        when(spyChannelManager.findOrCreateConsumer(messageOptions.getNamespace())).thenReturn(consumer);

        ResponderServer responderServer = ResponderServer
                .create(messageOptions, spyMsbContext);

        ResponderServer spyResponderServer = spy(responderServer);

        spyResponderServer
                .use(middleware)
                .listen();

        Message originalMessage = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(messageOptions.getNamespace());
        consumer.handleRawMessage(Utils.toJson(originalMessage));

        verify(spyResponderServer).onResponder(anyObject());
    }

    @Test
    public void testResponderServerAsyncProcessWithError() throws Exception {

        Exception error = new Exception();
        Middleware middleware = (request, responder) -> { throw error; };

        ResponderServer responderServer = ResponderServer
                .create(messageOptions, msbContext)
                .use(middleware)
                .listen();

        // simulate incoming request
        ArgumentCaptor<Payload> responseCaptor = ArgumentCaptor.forClass(Payload.class);
        Responder responder = spy(new Responder(messageOptions, TestUtils.createMsbRequestMessageNoPayload(), msbContext));

        CountDownLatch awaitSendResponse = new CountDownLatch(1);
        doAnswer(invocation -> {
                    awaitSendResponse.countDown();
                    return awaitSendResponse;
                }
        ).when(responder).send(responseCaptor.capture());

        responderServer.onResponder(responder);

        awaitSendResponse.await(2000, TimeUnit.MILLISECONDS);
        assertEquals(500, responseCaptor.getValue().getStatusCode().intValue());
    }
}
