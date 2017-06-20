package io.github.tcdl.msb.cli;

import com.google.common.collect.Sets;
import io.github.tcdl.msb.adapters.AdapterFactory;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.api.AmqpResponderOptions;
import io.github.tcdl.msb.api.ExchangeType;
import io.github.tcdl.msb.api.ResponderOptions;

import java.util.HashSet;
import java.util.Set;

/**
 * {@link CliMessageSubscriber} uses raw {@link ConsumerAdapter} (that doesn't validate or parse incoming messages)
 */
public class CliMessageSubscriber {
    private AdapterFactory adapterFactory;
    private final Set<String> registeredTopics = new HashSet<>();

    public CliMessageSubscriber(AdapterFactory adapterFactory) {
        this.adapterFactory = adapterFactory;
    }

    /**
     * Subscribes the given handler to the given topic
     */
    public void subscribe(String topicName, ExchangeType exchangeType, CliMessageHandler handler) {
        synchronized (registeredTopics) {
            if (!registeredTopics.contains(topicName)) {
                ResponderOptions responderOptions = new AmqpResponderOptions.Builder()
                        .withExchangeType(exchangeType)
                        .withBindingKeys(Sets.newHashSet("*"))
                        .build();
                ConsumerAdapter adapter = adapterFactory.createConsumerAdapter(topicName, responderOptions, false);

                adapter.subscribe(handler);
                registeredTopics.add(topicName);
            }
        }
    }
}
