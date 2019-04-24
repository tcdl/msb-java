package io.github.tcdl.msb;

import com.typesafe.config.ConfigFactory;
import io.github.tcdl.msb.api.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class ActiveMQRequesterResponderTest {

    private String namespace = "activemq:req-resp:test";
    private MsbContext msbContext;
    private ResponderServer responderServer;

    @Before
    public void setUp() {
        msbContext = new MsbContextBuilder()
                .enableShutdownHook(true)
                .withConfig(ConfigFactory.load())
                .build();
    }

    @After
    public void tearDown() throws InterruptedException {
        responderServer.stop();
        msbContext.shutdown();
        TimeUnit.SECONDS.sleep(5);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void requestResponseTest() throws Exception {
        String message = "test message";

        RequestOptions requestOptions = new ActiveMQRequestOptions.Builder()
                .withWaitForResponses(1)
                .build();

        ResponderOptions responderOptions = new ActiveMQResponderOptions.Builder()
                .build();

        BiConsumer<String, MessageContext> responseHandlerMock = mock(BiConsumer.class);

        responderServer = msbContext.getObjectFactory().createResponderServer(namespace, responderOptions,
                (request, responderContext) -> {
                    responderContext.getResponder().send(request);
                }, String.class)
                .listen();

        msbContext.getObjectFactory().createRequester(namespace, requestOptions, String.class)
                .onResponse(responseHandlerMock)
                .publish(message);

        verify(responseHandlerMock, timeout(5000).times(1)).accept(eq(message), any(MessageContext.class));
    }
}
