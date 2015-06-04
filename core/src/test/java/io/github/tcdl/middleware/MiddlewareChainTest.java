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
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;

/**
 * Created by rdro on 5/22/2015.
 */
@RunWith(MockitoJUnitRunner.class)
public class MiddlewareChainTest {

    private MsbContext msbContext;
    
    @Mock
    private Middleware middlewareMock;
    
    @Before
    public void setUp() {
        this.msbContext = TestUtils.createSimpleMsbContext();
    }

    @Test
    public void testExecuteSuccess() throws Exception {

        MiddlewareChain middlewareChain = new MiddlewareChain();
        middlewareChain.add(middlewareMock);

        MsbMessageOptions msgOptions = TestUtils.createSimpleConfig();
        Message message = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo("middleware:testexecute-sucess");
        Payload request = message.getPayload();
        Responder responder = new Responder(msgOptions, message, msbContext);

        middlewareChain.invoke(request, responder);

        verify(middlewareMock, only())
                .execute(eq(request), eq(responder), eq(middlewareChain));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testExecuteWithError() throws Exception {
        MiddlewareHandler errorHandlerMock = mock(MiddlewareHandler.class);

        MiddlewareChain middlewareChain = new MiddlewareChain().withErrorHandler(errorHandlerMock);
        middlewareChain.add(middlewareMock);

        MsbMessageOptions msgOptions = TestUtils.createSimpleConfig();
        Message message = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo("middleware:testexecute-witherror");
        Payload request = message.getPayload();
        Responder responder = new Responder(msgOptions, message, msbContext);

        doThrow(Exception.class).when(middlewareMock)
                .execute(any(), any(), any());

        middlewareChain.invoke(request, responder);

        verify(middlewareMock, only())
                .execute(eq(request), eq(responder), eq(middlewareChain));
        verify(errorHandlerMock, only())
                .handle(eq(request), eq(responder), any());
    }
}
