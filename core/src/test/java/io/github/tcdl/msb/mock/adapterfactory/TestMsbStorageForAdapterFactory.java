package io.github.tcdl.msb.mock.adapterfactory;

import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.api.MsbContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class provides storage and accessors for {@link TestMsbAdapterFactory}-based
 * testing.
 */
public class TestMsbStorageForAdapterFactory {

    private final HashMap<String, Map<Set<String>, TestMsbConsumerAdapter>> multicastConsumers = new HashMap<>();
    private final HashMap<String, TestMsbConsumerAdapter> broadcastConsumers = new HashMap<>();
    private final HashMap<String, TestMsbProducerAdapter> producers = new HashMap<>();
    private final HashMap<String, Map<String, List<String>>> publishedMessages = new HashMap<>();

    /**
     * Get TestMsbStorageForAdapterFactory instance used by a MsbContext.
     *
     * @param msbContext
     * @return
     */
    public static TestMsbStorageForAdapterFactory extract(MsbContext msbContext) {
        try {
            ChannelManager channelManager = (ChannelManager) FieldUtils.readField(msbContext, "channelManager", true);
            TestMsbAdapterFactory adapterFactory = (TestMsbAdapterFactory) FieldUtils.readField(channelManager, "adapterFactory", true);
            return adapterFactory.getStorage();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Force other context to use this TestMsbStorageForAdapterFactory instance so messaging will be shared
     * between different contexts. Without this action, an MsbContext handles its own messages only.
     *
     * @param otherContext
     */
    public void connect(MsbContext otherContext) {
        try {
            ChannelManager channelManager = (ChannelManager) FieldUtils.readField(otherContext, "channelManager", true);
            TestMsbAdapterFactory adapterFactory = (TestMsbAdapterFactory) FieldUtils.readField(channelManager, "adapterFactory", true);
            adapterFactory.setStorage(this);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    synchronized void addProducerAdapter(String namespace, TestMsbProducerAdapter adapter) {
        producers.put(namespace, adapter);
    }

    synchronized void addConsumerAdapter(String namespace, Set<String> routingKeys, TestMsbConsumerAdapter adapter) {
        multicastConsumers.computeIfAbsent(namespace, ns -> new HashMap<>()).replace(routingKeys, adapter);
    }

    synchronized void addConsumerAdapter(String namespace, TestMsbConsumerAdapter adapter) {
        broadcastConsumers.putIfAbsent(namespace, adapter);
    }

    synchronized void addPublishedTestMessage(String namespace, String routingKey, String jsonMessage) {
        publishedMessages.computeIfAbsent(namespace, ns -> new HashMap<>())
                .computeIfAbsent(routingKey, rk -> new ArrayList<>())
                .add(jsonMessage);
    }

    /**
     * Reset the storage.
     */
    public synchronized void cleanup() {
        multicastConsumers.clear();
        broadcastConsumers.clear();
        producers.clear();
        publishedMessages.clear();
    }

    /**
     * Publish a raw JSON message that should be handled as an incoming message.
     */
    public synchronized void publishIncomingMessage(String namespace, String routingKey, String jsonMessage) {
        if (broadcastConsumers.get(namespace) != null) {
            broadcastConsumers.get(namespace).pushTestMessage(jsonMessage);
        } else {
            multicastConsumers.getOrDefault(namespace, Collections.emptyMap()).entrySet().stream()
                    .filter(entry -> entry.getKey().contains(routingKey))
                    .forEach(entry -> entry.getValue().pushTestMessage(jsonMessage));
        }
    }

    /**
     * Get a list of outgoing raw JSON messages.
     */
    public synchronized List<String> getOutgoingMessages(String namespace) {
        return publishedMessages.getOrDefault(namespace, Collections.emptyMap())
                .values()
                .stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    /**
     * Get a list of outgoing raw JSON messages.
     */
    public synchronized List<String> getOutgoingMessages(String namespace, Set<String> routingKeys) {
        Validate.notNull(routingKeys, "routingKeys should not be null");
        return publishedMessages.getOrDefault(namespace, Collections.emptyMap())
                .entrySet()
                .stream()
                .filter(entry -> routingKeys.contains(entry.getKey()))
                .flatMap(entry -> entry.getValue().stream())
                .collect(Collectors.toList());
    }

    /**
     * Get a single outgoing raw JSON message.
     */
    public synchronized String getOutgoingMessage(String namespace) {
        return this.getOutgoingMessage(namespace, StringUtils.EMPTY);
    }

    public synchronized String getOutgoingMessage(String namespace, String routingKey) {
        List<String> messages = publishedMessages.getOrDefault(namespace, Collections.emptyMap())
                .getOrDefault(routingKey, Collections.emptyList());

        if (messages == null || messages.size() != 1) {
            throw new RuntimeException("A single outgoing message is expected");
        }
        return messages.get(0);
    }
}
