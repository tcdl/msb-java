package io.github.tcdl.msb.acceptance;

import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import io.github.tcdl.msb.api.*;
import org.junit.After;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class RoutingKeysTest {

    private static final String NAMESPACE = "test:routing";

    @After
    public void tearDown() throws Exception {
        MsbTestHelper.getInstance().shutdownAll();
    }

    @Test
    public void consumerReceivesMessagesFilteredByRoutingKeys_ifRoutingKeysAreSupported() throws Exception {

        Config baseConfig = ConfigFactory.load();

        MsbContext responder1Context = distinctContext(baseConfig);
        MsbContext responder2Context = distinctContext(baseConfig);

        MsbContext requesterContext = distinctContext(baseConfig);

        String routingKey1 = "routing-key-1";
        String routingKey2 = "routing-key-2";
        String routingKey3 = "routing-key-3";

        String message1 = "message1";
        String message2 = "message2";
        String message3 = "message3";

        CompletableFuture<?> deferredResult1 = new CompletableFuture<>();
        CompletableFuture<?> deferredResult2 = new CompletableFuture<>();
        CompletableFuture<?> deferredResult3 = new CompletableFuture<>();

        ConcurrentMap<String, CompletableFuture<?>> deferredResults1 = Maps.newConcurrentMap();
        deferredResults1.put(message1, deferredResult1);
        deferredResults1.put(message2, deferredResult2);
        setUpResponderForRoutingKeys(responder1Context, newHashSet(routingKey1, routingKey2), arrivingMessagesChecker(deferredResults1));

        ConcurrentMap<String, CompletableFuture<?>> deferredResults2 = Maps.newConcurrentMap();
        deferredResults2.put(message3, deferredResult3);
        setUpResponderForRoutingKeys(responder2Context, newHashSet(routingKey3), arrivingMessagesChecker(deferredResults2));

        //publish messages with different routing keys
        publishMessage(requesterContext, ExchangeType.TOPIC, routingKey1, message1);
        publishMessage(requesterContext, ExchangeType.TOPIC, routingKey2, message2);
        publishMessage(requesterContext, ExchangeType.TOPIC, routingKey3, message3);

        //wait for at most 1 second and check
        CompletableFuture<Void> combinedDeferredResult = CompletableFuture.allOf(deferredResult1, deferredResult2, deferredResult3);
        combinedDeferredResult.get(1, TimeUnit.SECONDS);
        assertFalse(combinedDeferredResult.isCancelled());
    }

    @Test
    public void consumerReceivesAllMessages_ifRoutingKeysAreNotSupported() throws Exception {
        Config baseConfig = ConfigFactory.load();

        MsbContext responder1Context = distinctContext(baseConfig);
        MsbContext responder2Context = distinctContext(baseConfig);

        MsbContext requesterContext = distinctContext(baseConfig);

        String routingKey1 = "routing-key-1";
        String routingKey2 = "routing-key-2";

        String message1 = "message1";
        String message2 = "message2";

        CountDownLatch expectedMessagesCountDown = new CountDownLatch(4); //2 for each consumer

        //set up fanout exchange with two queues bound using routing keys
        responder1Context.getObjectFactory().createResponderServer(NAMESPACE, new ResponderOptions.Builder().withBindingKeys(newHashSet(routingKey1)).build(),
                (request, responderContext) -> expectedMessagesCountDown.countDown(),
                String.class)
                .listen();

        responder2Context.getObjectFactory().createResponderServer(NAMESPACE, new ResponderOptions.Builder().withBindingKeys(newHashSet(routingKey1)).build(),
                (request, responderContext) -> expectedMessagesCountDown.countDown(),
                String.class)
                .listen();

        //publish messages with different routing keys
        publishMessage(requesterContext, ExchangeType.FANOUT, routingKey1, message1);
        publishMessage(requesterContext, ExchangeType.FANOUT, routingKey2, message2);

        //check that all messages where received by all consumers
        assertTrue("", expectedMessagesCountDown.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void consumerByDefaultReceivesMessages() throws Exception {
        Config baseConfig = ConfigFactory.load();
        MsbContext requesterContext = distinctContext(baseConfig);
        MsbContext responderContext = distinctContext(baseConfig);

        CountDownLatch expectedMessagesCountDown = new CountDownLatch(1);

        //routing key not specified
        ResponderOptions responderOptions = new AmqpResponderOptions.Builder().withExchangeType(ExchangeType.TOPIC).build();

        responderContext.getObjectFactory().createResponderServer(NAMESPACE, responderOptions,
                (request, responderContext1) -> expectedMessagesCountDown.countDown(), String.class)
                .listen();

        RequestOptions requestOptions = new AmqpRequestOptions.Builder().withExchangeType(ExchangeType.TOPIC).withRoutingKey("some.routing.key").build();
        requesterContext.getObjectFactory().createRequesterForFireAndForget(NAMESPACE, requestOptions).publish("message");

        assertTrue(expectedMessagesCountDown.await(1, TimeUnit.SECONDS));
    }

    private Consumer<String> arrivingMessagesChecker(ConcurrentMap<String, CompletableFuture<?>> deferredResults) {
        return (message) -> {
            if (deferredResults.containsKey(message)) {
                deferredResults.get(message).complete(null); //value is irrelevant
            } else {
                deferredResults.values().forEach(result -> result.cancel(false));
            }
        };
    }

    private MsbContext distinctContext(Config baseConfig) {
        return MsbTestHelper.getInstance().initDistinctContext(MsbTestHelper.temporaryInfrastructure(baseConfig));
    }

    private void publishMessage(MsbContext requesterContext, ExchangeType exchangeType, String routingKey, String message) {
        RequestOptions requestOptions = new AmqpRequestOptions.Builder()
                .withExchangeType(exchangeType)
                .withRoutingKey(routingKey)
                .build();
        requesterContext.getObjectFactory().createRequester(NAMESPACE, requestOptions, String.class).publish(message);
    }

    private void setUpResponderForRoutingKeys(MsbContext context, Set<String> bindingKeys, Consumer<String> messageHandler) {
        ResponderOptions responderOptions = new AmqpResponderOptions.Builder()
                .withExchangeType(ExchangeType.TOPIC)
                .withBindingKeys(bindingKeys).build();

        context.getObjectFactory().createResponderServer(NAMESPACE, responderOptions,
                (request, responderContext) -> messageHandler.accept(request),
                String.class)
                .listen();
    }
}
