
package io.github.tcdl.msb;

import com.google.common.collect.Sets;
import com.typesafe.config.ConfigFactory;
import io.github.tcdl.msb.api.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;

public class ActiveMQRoutingKeyTest {

    private String namespace = "activemq:rounting-keys:test";
    private MsbContext msbContext;
    private ResponderServer responderServer;
    private TransferQueue<String> transferQueue;

    @Before
    public void setUp() {
        msbContext = new MsbContextBuilder()
                .enableShutdownHook(true)
                .withConfig(ConfigFactory.load())
                .build();
        transferQueue = new LinkedTransferQueue<>();
    }

    @After
    public void tearDown() throws InterruptedException {
        responderServer.stop();
        msbContext.shutdown();
        transferQueue.clear();
        TimeUnit.SECONDS.sleep(5);
    }

    @Test
    public void requestResponseWithRoutingKeyTest() throws InterruptedException {
        final String message = "test message";
        final String routingKey = "RK";

        RequestOptions requestOptions = new ActiveMQRequestOptions.Builder()
                .withSubscriptionType(SubscriptionType.TOPIC)
                .withRoutingKey(routingKey)
                .withWaitForResponses(0)
                .build();

        ResponderOptions responderOptions = new ActiveMQResponderOptions.Builder()
                .withSubscriptionType(SubscriptionType.QUEUE)
                .withBindingKeys(Sets.newHashSet(routingKey))
                .build();

        responderServer = msbContext.getObjectFactory().createResponderServer(namespace, responderOptions,
                (request, responderContext) -> transferQueue.tryTransfer(message), String.class)
                .listen();

        TimeUnit.SECONDS.sleep(5);

        msbContext.getObjectFactory().createRequester(namespace, requestOptions)
                .publish(message);

        String receivedMessage = transferQueue.poll(5, TimeUnit.SECONDS);

        Assert.assertEquals(message, receivedMessage);
    }
}
