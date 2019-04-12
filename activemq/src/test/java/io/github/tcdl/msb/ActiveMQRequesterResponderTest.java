package io.github.tcdl.msb;

import com.google.common.collect.Sets;
import com.typesafe.config.ConfigFactory;
import io.github.tcdl.msb.api.*;
import io.github.tcdl.msb.api.message.Message;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ActiveMQRequesterResponderTest {

    private String namespace = "activemq:test";
    private MsbContext msbContext;
    private TransferQueue<String> transferQueue;
    private ResponderServer responderServer;

    @Before
    public void setUp() {
        msbContext = new MsbContextBuilder()
                .enableShutdownHook(true)
                .withConfig(ConfigFactory.load())
                .build();
        transferQueue = new LinkedTransferQueue<>();
    }

    @After
    public void tearDown() {
        responderServer.stop();
        msbContext.shutdown();
        transferQueue.clear();
    }

    @Test
    public void requestResponseTest() throws InterruptedException {
        String message = "test message";

        RequestOptions requestOptions = new ActiveMQRequestOptions.Builder()
                .withSubscriptionType(SubscriptionType.TOPIC)
                .build();

        ResponderOptions responderOptions = new ActiveMQResponderOptions.Builder()
                .withSubscriptionType(SubscriptionType.QUEUE)
                .build();

        responderServer = msbContext.getObjectFactory().createResponderServer(namespace, responderOptions,
                (request, responderContext) -> transferQueue.tryTransfer(message), String.class)
                .listen();

        msbContext.getObjectFactory().createRequester(namespace, requestOptions)
                .publish(message);

        String receivedMessage = transferQueue.poll(10, TimeUnit.SECONDS);

        Assert.assertEquals(message, receivedMessage);
    }

    @Test
    public void requestResponseWithRoutingKeyTest() throws InterruptedException {
        String message = "test message";
        String routingKey = "RK";

        RequestOptions requestOptions = new ActiveMQRequestOptions.Builder()
                .withSubscriptionType(SubscriptionType.TOPIC)
                .withRoutingKey(routingKey)
                .build();

        ResponderOptions responderOptions = new ActiveMQResponderOptions.Builder()
                .withSubscriptionType(SubscriptionType.QUEUE)
                .withBindingKeys(Sets.newHashSet(routingKey))
                .build();

        responderServer = msbContext.getObjectFactory().createResponderServer(namespace, responderOptions,
                (request, responderContext) -> transferQueue.tryTransfer(message), String.class)
                .listen();

        msbContext.getObjectFactory().createRequester(namespace, requestOptions)
                .publish(message);

        String receivedMessage = transferQueue.poll(10, TimeUnit.SECONDS);

        Assert.assertEquals(message, receivedMessage);
    }

    @Test
    public void requestResponseErrorHandlerTest()  throws InterruptedException {
        String message = "test message";

        RequestOptions requestOptions = new ActiveMQRequestOptions.Builder()
                .withSubscriptionType(SubscriptionType.TOPIC)
                .build();

        ResponderOptions responderOptions = new ActiveMQResponderOptions.Builder()
                .withSubscriptionType(SubscriptionType.QUEUE)
                .build();

        ResponderServer.ErrorHandler errorHandlerMock = mock(ResponderServer.ErrorHandler.class);

        responderServer = msbContext.getObjectFactory().createResponderServer(namespace, responderOptions,
                (request, responderContext) -> {
                    transferQueue.tryTransfer(message);
                    throw new RuntimeException("Error");
                }, errorHandlerMock, String.class)
                .listen();

        msbContext.getObjectFactory().createRequester(namespace, requestOptions)
                .publish(message);

        transferQueue.poll(10, TimeUnit.SECONDS);

        verify(errorHandlerMock).handle(any(RuntimeException.class), any(Message.class));
    }
}
