package io.github.tcdl.middleware;

import io.github.tcdl.Responder;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.function.Consumer;

/**
 * Created by rdrozdov-tc on 6/10/15.
 */
@RunWith(MockitoJUnitRunner.class)
public class MiddlewareTest {

    @Mock
    private Responder responderMock;

    @Test
    public void testExecuteSuccess() throws Exception {
        Consumer<Payload> middewareBody = Mockito.mock(Consumer.class);
        Payload request = TestUtils.createSimpleRequestPayload();
        Middleware middleware = (req, responder) -> {
            middewareBody.accept(req);
        };
        middleware.execute(request, responderMock);
        Mockito.verify(middewareBody).accept(Mockito.eq(request));
    }

    @Test(expected = NullPointerException.class)
    public void testExecuteWithException() throws Exception {
        Payload request = TestUtils.createSimpleRequestPayload();
        Middleware middleware = (req, responder) -> {
            throw new NullPointerException();
        };
        middleware.execute(request, responderMock);
    }
}
