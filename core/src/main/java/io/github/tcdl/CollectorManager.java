package io.github.tcdl;

import io.github.tcdl.messages.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages instances of {@link Collector}s that listen to the same response topics.
 */
public class CollectorManager implements MessageHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CollectorManager.class);

    private String topic;
    private ChannelManager channelManager;
    Map<String, Collector> collectorsByCorrelationId  = new ConcurrentHashMap<>();

    public CollectorManager(String topic, ChannelManager channelManager) {
        this.topic = topic;
        this.channelManager = channelManager;
    }

    /**
     * Determines correlation id from the incoming message and invokes the relevant {@link Collector} instance
     */
    @Override
    public void handleMessage(Message message) {
        String correlationId = message.getCorrelationId();
        Collector collector = collectorsByCorrelationId.get(correlationId);
        if (collector != null) {
            collector.handleMessage(message);
        } else {
            LOG.warn("Message with correlationId {} is not expected to be processed by any Collectors", correlationId) ;
        }

    }

    public void registerCollector(Collector collector) {
        collectorsByCorrelationId.putIfAbsent(collector.getRequestMessage().getCorrelationId(), collector);
    }

    /**
     * Unsubscribe from consumer if no more collectors await on consumer topic.
     */
    public synchronized void unsubscribe(Collector collector) {
        collectorsByCorrelationId.remove(collector.getRequestMessage().getCorrelationId());

        if (collectorsByCorrelationId.isEmpty()) {
            channelManager.unsubscribe(topic);
        }
    }
}
