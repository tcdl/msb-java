package io.github.tcdl.msb.examples;

import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.api.MsbContextBuilder;
import io.github.tcdl.msb.api.RequestOptions;
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
 * Run this consumer in conjunction with {@link ConsumerWithRoutingKeys} to see that only messages sent with
 * routing keys 'zero' and 'two' are consumed and the messages sent with routing key 'one' are not.
 */
public class ProducerWithRoutingKey {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerWithRoutingKey.class);

    public static void main(String[] args) {
        MsbContext msbContext = new MsbContextBuilder().
                enableShutdownHook(true).
                build();

        Map<Integer, String> tikToRoutingKey = new HashMap<>(3);
        tikToRoutingKey.put(0, "zero");
        tikToRoutingKey.put(1, "one");
        tikToRoutingKey.put(2, "two");

        MessageTemplate messageTemplate = new MessageTemplate().withTags("publish-with-routing-key");

        AtomicInteger tik = new AtomicInteger(0);

        Runnable task = () -> {

            String routingKey = tikToRoutingKey.get(tik.getAndIncrement() % 3);
            RequestOptions requestOptions = new RequestOptions.Builder()
                    .withRoutingKey(routingKey)
                    .withMessageTemplate(messageTemplate)
                    .build();

            LOG.info("Sending message with routing key '{}'", routingKey);
            msbContext.getObjectFactory()
                    .createRequester("routing:namespace", requestOptions, String.class)
                    .publish(routingKey.toUpperCase(), UUID.randomUUID().toString());
        };

        runEachNSeconds(2, task);
    }

    private static void runEachNSeconds(int secondsNumber, Runnable task) {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                executor.shutdownNow();
            }
        });

        executor.scheduleAtFixedRate(task, 0, secondsNumber, TimeUnit.SECONDS);
    }
}
