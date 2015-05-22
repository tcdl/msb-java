package io.github.tcdl.adapters;

import io.github.tcdl.exception.ChannelException;
import io.github.tcdl.exception.JsonConversionException;
import io.github.tcdl.messages.Message;
import io.github.tcdl.support.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by rdro on 4/24/2015.
 */
public class MockAdapter implements Adapter {

    public static final Logger LOG = LoggerFactory.getLogger(MockAdapter.class);

    private static Map<String, Queue<String>> messageMap = new ConcurrentHashMap<String, Queue<String>>();

    private String topic;

    public MockAdapter(String topic) {
        this.topic = topic;
    }

    @Override
    public void publish(String jsonMessage) throws ChannelException {
        LOG.info("Recieved request {}", jsonMessage);
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
            LOG.error("Recieved message can not be parsed");
        }
    }

    @Override
    public void subscribe(RawMessageHandler messageHandler) {

        LOG.info("Queue for topic {} contains messages: {}", topic, messageMap.get(topic));
        if (messageMap.get(topic) != null) {
            String jsonMessage = messageMap.get(topic).poll();
            if (messageHandler != null && jsonMessage != null) {
                messageHandler.onMessage(jsonMessage);
            }
        }
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

}
