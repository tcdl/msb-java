package io.github.tcdl.msb.examples;

import io.github.tcdl.msb.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Run this producer in conjunction with {@link ConsumerWithRoutingKeys} to see that only messages sent with
 * routing keys 'zero' and 'two' are consumed and the messages sent with routing key 'one' are not.
 */
public class ProducerWithXConsistenceHash {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerWithXConsistenceHash.class);

    public static void main(String[] args) {
        MsbContext msbContext = new MsbContextBuilder().
                enableShutdownHook(true).
                build();

        MessageTemplate messageTemplate = new MessageTemplate().withTags("publish-with-routing-key");

        Runnable task = () -> {

            String routingKey = "87713271-296f-4a66-898c-024b963e006d";// UUID.randomUUID().toString();
            RequestOptions requestOptions = new AmqpRequestOptions.Builder()
                    .withExchangeType(ExchangeType.X_CONSISTENT_HASH)
                    .withRoutingKey(routingKey)
                    .withMessageTemplate(messageTemplate)
                    .build();

            LOG.info("Sending message with routing key '{}'", routingKey);
            msbContext.getObjectFactory()
                    .createRequester("consistence:exchange", requestOptions, String.class)
                    .publish(routingKey.toUpperCase(), UUID.randomUUID().toString());
        };

        runEachNSeconds(2, task);
    }

    private static void runEachNSeconds(int secondsNumber, Runnable task) {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> executor.shutdownNow()));

        executor.scheduleAtFixedRate(task, 0, secondsNumber, TimeUnit.SECONDS);
    }
}
