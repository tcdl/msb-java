package io.github.tcdl.msb;

import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import io.github.tcdl.msb.api.*;
import io.github.tcdl.msb.api.message.Message;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.stream.IntStream;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ActiveMQRequesterResponderTest {

    private String namespace = "activemq:test";
    private List<MsbContext> msbContexts;
    private TransferQueue<String> transferQueue;
    private List<ResponderServer> responderServers;

    @Before
    public void setUp() {
        responderServers = new LinkedList<>();
        msbContexts = new LinkedList<>();
        msbContexts.add(new MsbContextBuilder()
                .enableShutdownHook(true)
                .withConfig(ConfigFactory.load())
                .build());
        transferQueue = new LinkedTransferQueue<>();
    }

    @After
    public void tearDown() {
        responderServers.forEach(ResponderServer::stop);
        msbContexts.forEach(MsbContext::shutdown);
        transferQueue.clear();
        responderServers.clear();
    }

    @Test
    public void requestResponseTest() throws InterruptedException {
        String message = "test message";

        MsbContext msbContext = msbContexts.get(0);

        RequestOptions requestOptions = new ActiveMQRequestOptions.Builder()
                .withSubscriptionType(SubscriptionType.TOPIC)
                .withWaitForResponses(0)
                .build();

        ResponderOptions responderOptions = new ActiveMQResponderOptions.Builder()
                .withSubscriptionType(SubscriptionType.QUEUE)
                .build();

        responderServers.add(msbContext.getObjectFactory().createResponderServer(namespace, responderOptions,
                (request, responderContext) -> transferQueue.tryTransfer(message), String.class)
                .listen());

        msbContext.getObjectFactory().createRequester(namespace, requestOptions)
                .publish(message);

        String receivedMessage = transferQueue.poll(5, TimeUnit.SECONDS);

        Assert.assertEquals(message, receivedMessage);
    }

    @Test
    public void requestResponseWithRoutingKeyTest() throws InterruptedException {
        final String message = "test message";
        final String routingKey = "RK";

        MsbContext msbContext = msbContexts.get(0);

        RequestOptions requestOptions = new ActiveMQRequestOptions.Builder()
                .withSubscriptionType(SubscriptionType.TOPIC)
                .withRoutingKey(routingKey)
                .withWaitForResponses(0)
                .build();

        ResponderOptions responderOptions = new ActiveMQResponderOptions.Builder()
                .withSubscriptionType(SubscriptionType.QUEUE)
                .withBindingKeys(Sets.newHashSet(routingKey))
                .build();

        responderServers.add(msbContext.getObjectFactory().createResponderServer(namespace, responderOptions,
                (request, responderContext) -> transferQueue.tryTransfer(message), String.class)
                .listen());

        msbContext.getObjectFactory().createRequester(namespace, requestOptions)
                .publish(message);

        String receivedMessage = transferQueue.poll(5, TimeUnit.SECONDS);

        Assert.assertEquals(message, receivedMessage);
    }

    @Test
    public void requestResponseErrorHandlerTest()  throws InterruptedException {
        final String message = "test message";

        MsbContext msbContext = msbContexts.get(0);

        RequestOptions requestOptions = new ActiveMQRequestOptions.Builder()
                .withSubscriptionType(SubscriptionType.TOPIC)
                .withWaitForResponses(0)
                .build();

        ResponderOptions responderOptions = new ActiveMQResponderOptions.Builder()
                .withSubscriptionType(SubscriptionType.QUEUE)
                .build();

        ResponderServer.ErrorHandler errorHandlerMock = mock(ResponderServer.ErrorHandler.class);

        responderServers.add(msbContext.getObjectFactory().createResponderServer(namespace, responderOptions,
                (request, responderContext) -> {
                    transferQueue.tryTransfer(message);
                    throw new RuntimeException("Error");
                }, errorHandlerMock, String.class)
                .listen());

        msbContext.getObjectFactory().createRequester(namespace, requestOptions)
                .publish(message);

        transferQueue.poll(5, TimeUnit.SECONDS);

        verify(errorHandlerMock).handle(any(RuntimeException.class), any(Message.class));
    }

    @Test
    public void multipleConsumersTest()  throws InterruptedException {
        final int numberOfConsumers = 5;
        final String message = "test message";

        MsbContext msbContext = msbContexts.get(0);

        RequestOptions requestOptions = new ActiveMQRequestOptions.Builder()
                .withSubscriptionType(SubscriptionType.TOPIC)
                .withWaitForResponses(0)
                .build();

        ResponderOptions responderOptions = new ActiveMQResponderOptions.Builder()
                .withSubscriptionType(SubscriptionType.QUEUE)
                .build();

        CountDownLatch consumedLatch = new CountDownLatch(numberOfConsumers);
        IntStream.range(0, numberOfConsumers).forEach((i) -> {
            Config config = getConfigWith(ConfigFactory.load(), "msbConfig.brokerConfig.groupId", "consumer" + (i+1));

            MsbContext consumerMsbContext = new MsbContextBuilder()
                    .enableShutdownHook(true)
                    .withConfig(config)
                    .build();

            msbContexts.add(consumerMsbContext);

            responderServers.add(consumerMsbContext.getObjectFactory().createResponderServer(namespace, responderOptions,
                    (request, responderContext) -> consumedLatch.countDown(), String.class)
                    .listen());
        });
        msbContext.getObjectFactory().createRequester(namespace, requestOptions)
                .publish(message);

        consumedLatch.await(10, TimeUnit.SECONDS);

        Assert.assertEquals(0, consumedLatch.getCount());
    }

    private Config getConfigWith(Config config, String path, Object value) {
        ConfigValue configValue = ConfigFactory.parseString(path + "=\"" + value + "\"").getValue(path);
        return  config.withValue(path, configValue);
    }
}
