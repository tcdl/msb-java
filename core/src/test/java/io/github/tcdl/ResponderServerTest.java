package io.github.tcdl;

import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.middleware.Middleware;
import io.github.tcdl.middleware.MiddlewareChain;
import io.github.tcdl.support.TestUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

/**
 * Created by rdro on 4/30/2015.
 */
@RunWith(PowerMockRunner.class)
public class ResponderServerTest {

    private MsbMessageOptions messageOptions;
    private MsbContext msbContext = TestUtils.createSimpleMsbContext();


    @Before
    public void setUp() {
        messageOptions = TestUtils.createSimpleConfig();
        msbContext = TestUtils.createSimpleMsbContext();
    }

    @Test
    @PrepareForTest({ ResponderServer.class, Responder.class } )
    public void testResponderServerAsyncProcessSuccess() throws Exception {

        Middleware middleware = (request, responder) -> {};
        ResponderServer responderServer = ResponderServer
                .create(messageOptions, msbContext)
                .use(middleware)
                .listen();

        mockStatic(CompletableFuture.class);
        ArgumentCaptor<Supplier> middlewareCaptor = ArgumentCaptor.forClass(Supplier.class);

        // simulate incoming request
        responderServer.onResponder(new Responder(messageOptions, TestUtils.createMsbRequestMessageNoPayload(), msbContext));

        verifyStatic();
        CompletableFuture.supplyAsync(middlewareCaptor.capture());
        MiddlewareChain middlewareChain = (MiddlewareChain) middlewareCaptor.getValue().get();

        assertNotNull(middlewareChain);
        assertEquals(middleware, middlewareChain.getMiddleware().iterator().next());
    }

    @Test @Ignore("unstable")
    public void testResponderServerAsyncProcessWithError() throws Exception {

        Exception error = new Exception();
        Middleware middleware = (request, responder) -> { throw error; };

        ResponderServer responderServer = spy(ResponderServer
                .create(messageOptions, msbContext))
                .use(middleware)
                .listen();

        // simulate incoming request
        responderServer.onResponder(new Responder(messageOptions, TestUtils.createMsbRequestMessageNoPayload(), msbContext));

        verify(responderServer).errorHandler(any(), any(), eq(error));
    }
}
