package io.github.tcdl.msb.examples;

import com.google.common.collect.Sets;
import io.github.tcdl.msb.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run this consumer in conjunction with {@link ProducerWithRoutingKey} to see that only messages sent with
 * routing keys 'zero' and 'two' are consumed and the messages sent with routing key 'one' are not.
 */
public class ConsumerWithRoutingKeys {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerWithRoutingKeys.class);

    public static void main(String[] args) {
        MsbContext msbContext = new MsbContextBuilder().
                enableShutdownHook(true).
                build();


        ObjectFactory objectFactory = msbContext.getObjectFactory();

        ResponderOptions responderOptions = new AmqpResponderOptions.Builder()
                .withBindingKeys(Sets.newHashSet("zero", "two"))
                .withExchangeType(ExchangeType.TOPIC)
                .build();

        ResponderServer responderServer = objectFactory.createResponderServer("routing:namespace",
                responderOptions,
                (request, responderContext) -> {
                    LOG.info("Received message: {}", request);
                }, String.class);
        responderServer.listen();
    }
}
