package io.github.tcdl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;
import io.github.tcdl.events.Event;
import io.github.tcdl.middleware.Middleware;
import io.github.tcdl.middleware.MiddlewareChain;
import io.github.tcdl.support.TestUtils;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Created by rdro on 4/30/2015.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ ResponderServer.class, Responder.class } )
public class ResponderServerTest {

    @Test
    public void testResponderServerProcessSuccess() throws Exception {

        Middleware middleware = (request, responder) -> {};

        ResponderServer
                .create(TestUtils.createSimpleConfig())
                .use(middleware)
                .listen();

        mockStatic(CompletableFuture.class);
        ArgumentCaptor<Supplier> middlewareCaptor = ArgumentCaptor.forClass(Supplier.class);

        ChannelManager.getInstance().emit(Event.MESSAGE_EVENT, TestUtils.createMsbRequestMessageWithPayloadAndTopicTo("test:responser-server"));

        verifyStatic();
        CompletableFuture.supplyAsync(middlewareCaptor.capture());
        MiddlewareChain middlewareChain = (MiddlewareChain) middlewareCaptor.getValue().get();

        assertNotNull(middlewareChain);
        assertEquals(middleware, middlewareChain.getMiddleware().iterator().next());
    }
}
