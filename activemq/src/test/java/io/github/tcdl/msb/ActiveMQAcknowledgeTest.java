package io.github.tcdl.msb;

import com.typesafe.config.ConfigFactory;
import io.github.tcdl.msb.api.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class ActiveMQAcknowledgeTest {

    private String namespace = "activemq:acknowledge:test";
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
    public void retryMessageTest() throws Exception {
        String message = "test message";

        RequestOptions requestOptions = new ActiveMQRequestOptions.Builder()
                .withWaitForResponses(0)
                .build();

        ResponderOptions responderOptions = new ActiveMQResponderOptions.Builder()
                .build();

        CountDownLatch receivedMessageLatch = new CountDownLatch(1);
        responderServer = msbContext.getObjectFactory().createResponderServer(namespace, responderOptions,
                (request, responderContext) -> {
                    responderContext.getAcknowledgementHandler().retryMessage();
                    receivedMessageLatch.countDown();
                }, String.class)
                .listen();

        msbContext.getObjectFactory().createRequester(namespace, requestOptions, String.class)
                .publish(message);

        receivedMessageLatch.await(5, TimeUnit.SECONDS);

        Assert.assertEquals(0, receivedMessageLatch.getCount());

        // do restart
        responderServer.stop();

        // message should be returned to the queue and processed again
        ResponderServer.RequestHandler<String> handlerMock = mock(ResponderServer.RequestHandler.class);
        responderServer = msbContext.getObjectFactory().createResponderServer(namespace, responderOptions,
                handlerMock, String.class).listen();

        verify(handlerMock, timeout(5000).times(1)).process(eq(message), any(ResponderContext.class));
    }
}
