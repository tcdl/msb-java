package io.github.tcdl.msb.cli;

import io.github.tcdl.msb.adapters.AdapterFactory;
import io.github.tcdl.msb.adapters.ConsumerAdapter;

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
    public void subscribe(String topicName, CliMessageHandler handler) {
        synchronized (registeredTopics) {
            if (!registeredTopics.contains(topicName)) {
                ConsumerAdapter adapter = adapterFactory.createConsumerAdapter(topicName);
                adapter.subscribe(handler);
                registeredTopics.add(topicName);
            }
        }
    }
}
