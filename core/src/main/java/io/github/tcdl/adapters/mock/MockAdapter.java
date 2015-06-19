package io.github.tcdl.adapters.mock;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import io.github.tcdl.adapters.ConsumerAdapter;
import io.github.tcdl.adapters.ProducerAdapter;
import io.github.tcdl.exception.ChannelException;
import io.github.tcdl.exception.JsonConversionException;
import io.github.tcdl.messages.Message;
import io.github.tcdl.support.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MockAdapter class represents implementation of {@link ProducerAdapter} and {@link ConsumerAdapter}
 * for test purposes.
 */
public class MockAdapter implements ProducerAdapter, ConsumerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(MockAdapter.class);
    private static final int CONSUMING_INTERVAL = 20;

    static Map<String, Queue<String>> messageMap = new ConcurrentHashMap<>();

    private String topic;

    public MockAdapter(String topic) {
        this.topic = topic;
    }

    @Override
    public void publish(String jsonMessage) throws ChannelException {
        LOG.debug("Received request {}", jsonMessage);
        try {
            Message incomingMessage = Utils.fromJson(jsonMessage, Message.class);
            pushRequestMessage(incomingMessage);
        } catch (JsonConversionException e) {
            LOG.error("Received message can not be parsed");
        }
    }

    @Override
    public void subscribe(RawMessageHandler messageHandler) {

        Thread subscriberThread = new Thread(() -> {
            String jsonMessage = null;
            while (true) {
                jsonMessage = pollJsonMessageForTopic(topic);

                if (messageHandler != null && jsonMessage != null) {
                    LOG.debug("Process message for topic {} [{}]", topic, jsonMessage);
                    messageHandler.onMessage(jsonMessage);
                } else {
                    try {
                        Thread.sleep(CONSUMING_INTERVAL);
                    } catch (Exception e) {
                        LOG.debug("Finish listen for subscribed topic");
                    }
                }
            }
        });

        subscriberThread.setDaemon(true);
        subscriberThread.start();
    }

    @Override
    public void unsubscribe() {
        LOG.debug("Unsubscribe");
    }

    public static String pollJsonMessageForTopic(String topic) {
        String jsonMessage = null;
        if (messageMap.get(topic) != null) {
            jsonMessage = messageMap.get(topic).poll();
        }

        if (jsonMessage == null) {
            LOG.debug("No message found for topic {}", topic);
        }
        return jsonMessage;
    }

    public static void pushRequestMessage(Message message) {
        String topicTo = message.getTopics().getTo();
        Queue<String> messagesQueue = messageMap.get(topicTo);
        if (messagesQueue == null) {
            messagesQueue = new ConcurrentLinkedQueue<>();
            Queue<String> curQ = messageMap.putIfAbsent(topicTo, messagesQueue);
            if (curQ != null) {
                messagesQueue = curQ;
            }
        }
        try {
            String jsonMessage = Utils.toJson(message);
            messagesQueue.add(jsonMessage);
            LOG.debug("Message for topic {} published: [{}]", topicTo, jsonMessage);
        } catch (JsonConversionException e) {
            LOG.error("Pushed message can not be parsed");
        }
    }

}
