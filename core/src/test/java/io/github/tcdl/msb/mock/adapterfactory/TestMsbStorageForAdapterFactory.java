package io.github.tcdl.msb.mock.adapterfactory;

import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.api.MsbContext;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * This class provides statics-based storage and accessors for {@link TestMsbAdapterFactory}-based
 * testing.
 */
public class TestMsbStorageForAdapterFactory {
    private final HashMap<String, TestMsbConsumerAdapter> consumers = new HashMap<>();
    private final HashMap<String, TestMsbProducerAdapter> producers = new HashMap<>();
    private final HashMap<String, List<String>> publishedMessages = new HashMap<>();

    /**
     * Get TestMsbStorageForAdapterFactory instance used by a MsbContext.
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

    void addProducerAdapter(String namespace, TestMsbProducerAdapter adapter) {
        producers.put(namespace, adapter);
    }

    void addConsumerAdapter(String namespace, TestMsbConsumerAdapter adapter) {
        consumers.put(namespace, adapter);
    }

    void addPublishedTestMessage(String namespace, String jsonMessage) {
        List<String> publishedMessages = this.publishedMessages.get(namespace);
        if (publishedMessages == null) {
            publishedMessages = new ArrayList<>();
            this.publishedMessages.put(namespace, publishedMessages);
        }
        publishedMessages.add(jsonMessage);
    }


    /**
     * Reset the storage.
     */
    public void cleanup() {
        consumers.clear();
        producers.clear();
        publishedMessages.clear();
    }

    /**
     * Publish a raw JSON message that should be handled as an incoming message.
     * @param namespace
     * @param jsonMessage
     */
    public void publishIncomingMessage(String namespace, String jsonMessage) {
        TestMsbConsumerAdapter consumerAdapter = consumers.get(namespace);
        if(consumerAdapter != null) {
            consumerAdapter.pushTestMessage(jsonMessage);
        }
    }

    /**
     * Get a list of outgoing raw JSON messages.
     * @param namespace
     * @return
     */
    public List<String> getOutgoingMessages(String namespace) {
        List<String> publishedMessages = this.publishedMessages.get(namespace);
        if (publishedMessages == null) {
            publishedMessages = Collections.emptyList();
        }
        return publishedMessages;
    }

    /**
     * Get a single outgoing raw JSON message.
     * @param namespace
     * @return
     */
    public String getOutgoingMessage(String namespace) {
        List<String> publishedMessages = this.publishedMessages.get(namespace);
        if (publishedMessages == null || publishedMessages.size() != 1) {
            throw new RuntimeException("A single outgoing message is expected");
        }
        return publishedMessages.get(0);
    }

}
