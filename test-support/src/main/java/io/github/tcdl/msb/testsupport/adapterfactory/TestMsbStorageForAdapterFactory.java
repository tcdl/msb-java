package io.github.tcdl.msb.testsupport.adapterfactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * This class provides statics-based storage and accessors for {@link TestMsbAdapterFactory}-based
 * testing.
 */
public class TestMsbStorageForAdapterFactory {
    private final static HashMap<String, TestMsbConsumerAdapter> CONSUMERS = new HashMap<>();
    private final static HashMap<String, TestMsbProducerAdapter> PRODUCERS = new HashMap<>();
    private final static HashMap<String, List<String>> PUBLISHED_MESSAGES = new HashMap<>();

    static class Internal {

        static void addProducerAdapter(String namespace, TestMsbProducerAdapter adapter) {
            PRODUCERS.put(namespace, adapter);
        }

        static void addConsumerAdapter(String namespace, TestMsbConsumerAdapter adapter) {
            CONSUMERS.put(namespace, adapter);
        }

        static void addPublishedTestMessage(String namespace, String jsonMessage) {
            List<String> publishedMessages = PUBLISHED_MESSAGES.get(namespace);
            if (publishedMessages == null) {
                publishedMessages = new ArrayList<>();
                PUBLISHED_MESSAGES.put(namespace, publishedMessages);
                publishedMessages.add(jsonMessage);
            }
        }
    }

    /**
     * Reset the storage.
     */
    public static void cleanup() {
        CONSUMERS.clear();
        PRODUCERS.clear();
        PUBLISHED_MESSAGES.clear();
    }

    /**
     * Publish a raw JSON message that should be handled as an incoming message.
     * @param namespace
     * @param jsonMessage
     */
    public static void publishIncomingMessage(String namespace, String jsonMessage) {
        TestMsbConsumerAdapter consumerAdapter = CONSUMERS.get(namespace);
        if(consumerAdapter != null) {
            consumerAdapter.pushTestMessage(jsonMessage);
        }
    }

    /**
     * Get a list of outgoing raw JSON messages.
     * @param namespace
     * @return
     */
    public static List<String> getOutgoingMessages(String namespace) {
        List<String> publishedMessages = PUBLISHED_MESSAGES.get(namespace);
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
    public static String getOutgoingMessage(String namespace) {
        List<String> publishedMessages = PUBLISHED_MESSAGES.get(namespace);
        if (publishedMessages == null || publishedMessages.size() != 1) {
            throw new RuntimeException("A single outgoing message is expected");
        }
        return publishedMessages.get(0);
    }

}
