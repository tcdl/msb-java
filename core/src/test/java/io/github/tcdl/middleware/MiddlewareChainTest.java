package io.github.tcdl.middleware;

import io.github.tcdl.Responder;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.ThreeArgsEventHandler;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Created by rdro on 5/22/2015.
 */
@RunWith(MockitoJUnitRunner.class)
public class MiddlewareChainTest {

    @Mock
    private Middleware middlewareMock;

    @Test
    public void testExecuteSuccess() throws Exception {

        MiddlewareChain middlewareChain = new MiddlewareChain();
        middlewareChain.add(middlewareMock);

        MsbMessageOptions msgOptions = TestUtils.createSimpleConfig();
        Message message = TestUtils.createMsbRequestMessageWithPayload();
        Payload request = message.getPayload();
        Responder responder = new Responder(msgOptions, message);

        middlewareChain.invoke(request, responder);

        verify(middlewareMock, only())
                .execute(eq(request), eq(responder), eq(middlewareChain));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testExecuteWithError() throws Exception {
        ThreeArgsEventHandler<Payload, Responder, Exception> errorHandlerMock = mock(ThreeArgsEventHandler.class);

        MiddlewareChain middlewareChain = new MiddlewareChain().withErrorHandler(errorHandlerMock);
        middlewareChain.add(middlewareMock);

        MsbMessageOptions msgOptions = TestUtils.createSimpleConfig();
        Message message = TestUtils.createMsbRequestMessageWithPayload();
        Payload request = message.getPayload();
        Responder responder = new Responder(msgOptions, message);

        doThrow(Exception.class).when(middlewareMock)
                .execute(any(), any(), any());

        middlewareChain.invoke(request, responder);

        verify(middlewareMock, only())
                .execute(eq(request), eq(responder), eq(middlewareChain));
        verify(errorHandlerMock, only())
                .onEvent(eq(request), eq(responder), any());
    }
}
