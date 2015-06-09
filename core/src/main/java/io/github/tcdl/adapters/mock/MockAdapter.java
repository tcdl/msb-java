package io.github.tcdl.adapters.mock;

import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.exception.ChannelException;
import io.github.tcdl.exception.JsonConversionException;
import io.github.tcdl.messages.Message;
import io.github.tcdl.support.Utils;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MockAdapter class represents implementation of {@link Adapter}
 * for test purposes.
 * 
 */
public class MockAdapter implements Adapter {

    public static final Logger LOG = LoggerFactory.getLogger(MockAdapter.class);

    private static Map<String, Queue<String>> messageMap = new ConcurrentHashMap<String, Queue<String>>();

    private String topic;

    public MockAdapter(String topic, MsbConfigurations msbConfig) {
        this.topic = topic;
    }

    @Override
    public void publish(String jsonMessage) throws ChannelException {
        LOG.info("Received request {}", jsonMessage);
        try {
            Message incommingMessage = Utils.fromJson(jsonMessage, Message.class);
            String topicTo = incommingMessage.getTopics().getTo();
            Queue<String> messagesQueue = messageMap.get(topicTo);
            if (messagesQueue == null) {
                messagesQueue = new ConcurrentLinkedQueue<String>();
                Queue<String> curQ = messageMap.putIfAbsent(topicTo, messagesQueue);
                if (curQ != null) {
                    messagesQueue = curQ;
                }
            }
            messagesQueue.add(jsonMessage);
            LOG.info("Message for topic {} published: [{}]", topicTo, jsonMessage);
        } catch (JsonConversionException e) {
            LOG.error("Received message can not be parsed");
        }
    }

    @Override
    public void subscribe(RawMessageHandler messageHandler) {
        Thread subscriberThread = new Thread(() -> {
            while (true) {
                if (messageMap.get(topic) != null) {
                    String jsonMessage = messageMap.get(topic).poll();
                    if (messageHandler != null && jsonMessage != null) {
                        LOG.debug("Process message for topic {} [{}]", topic, jsonMessage);
                        messageHandler.onMessage(jsonMessage);
                    }
                } else {
                    try {
                        Thread.sleep(20);
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
        LOG.info("Unsubscribe");
    }

    public static String pollJsonMessageForTopic(String topic) {
        String jsonMessage = null;
        if (messageMap.get(topic) != null) {
            jsonMessage = messageMap.get(topic).poll();
            LOG.info("Polling message for topic {}: [{}]", topic, jsonMessage);
        }

        if (jsonMessage == null) {
            LOG.warn("No message found for topic {}", topic);
        }
        return jsonMessage;
    }

    public static void pushRequestMessage(Message message) {
        String topicTo = message.getTopics().getTo();
        Queue<String> messagesQueue = messageMap.get(topicTo);
        if (messagesQueue == null) {
            messagesQueue = new ConcurrentLinkedQueue<String>();
            Queue<String> curQ = messageMap.putIfAbsent(topicTo, messagesQueue);
            if (curQ != null) {
                messagesQueue = curQ;
            }
        }
        try {
            String jsonMessage = Utils.toJson(message);
            messagesQueue.add(jsonMessage);
            LOG.info("Message for topic {} published: [{}]", topicTo, jsonMessage);
        } catch (JsonConversionException e) {
            LOG.error("Pushed message can not be parsed");
        }
    }

}
