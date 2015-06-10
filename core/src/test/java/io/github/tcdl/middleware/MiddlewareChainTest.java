package io.github.tcdl.middleware;

import io.github.tcdl.MsbContext;
import io.github.tcdl.Responder;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Created by rdro on 5/22/2015.
 */
@RunWith(MockitoJUnitRunner.class)
public class MiddlewareChainTest {

    private MsbContext msbContext;
    
    @Mock
    private Middleware middlewareMock;

    private MsbMessageOptions msgOptions;
    private Message message;
    private Payload request;
    private Responder responder;

    @Before
    public void setUp() {
        this.msbContext = TestUtils.createSimpleMsbContext();

        msgOptions = TestUtils.createSimpleConfig();
        message = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo("middleware:testexecute-sucess");
        request = message.getPayload();
        responder = new Responder(msgOptions, message, msbContext);
    }

    @Test
    public void testInvokeSuccess() throws Exception {
        MiddlewareChain middlewareChain = new MiddlewareChain();
        middlewareChain.add(middlewareMock);

        middlewareChain.invoke(request, responder);

        verify(middlewareMock).execute(eq(request), eq(responder), eq(middlewareChain));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInvokeWithError() throws Exception {
        MiddlewareHandler errorHandlerMock = mock(MiddlewareHandler.class);

        MiddlewareChain middlewareChain = new MiddlewareChain().withErrorHandler(errorHandlerMock);
        middlewareChain.add(middlewareMock);

        Exception exception = new NullPointerException();
        doThrow(exception).when(middlewareMock)
                .execute(any(), any(), any());

        middlewareChain.invoke(request, responder);

        verify(middlewareMock).execute(eq(request), eq(responder), eq(middlewareChain));
        verify(errorHandlerMock).handle(eq(request), eq(responder), eq(exception));
    }

    @Test
    public void testExecuteSuccess() throws Exception {
        MiddlewareChain middlewareChain = new MiddlewareChain();
        middlewareChain.add(middlewareMock);

        middlewareChain.execute(request, responder);

        verify(middlewareMock).execute(eq(request), eq(responder), eq(middlewareChain));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testExecuteWithError() throws Exception {
        MiddlewareHandler errorHandlerMock = mock(MiddlewareHandler.class);

        MiddlewareChain middlewareChain = new MiddlewareChain().withErrorHandler(errorHandlerMock);
        middlewareChain.add(middlewareMock);

        Exception exception = new NullPointerException();
        doThrow(exception).when(middlewareMock)
                .execute(any(), any(), any());

        middlewareChain.execute(request, responder);

        verify(middlewareMock).execute(eq(request), eq(responder), eq(middlewareChain));
        verify(errorHandlerMock).handle(eq(request), eq(responder), eq(exception));
    }
}
