package io.github.tcdl.msb.collector;

import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.api.message.Message;
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
     * Determines correlationId from the incoming message and invokes the relevant {@link Collector} instance.
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

    public synchronized void registerCollector(Collector collector) {
        //make sure consumer is listening on topic
        channelManager.subscribe(topic, this);
        collectorsByCorrelationId.putIfAbsent(collector.getRequestMessage().getCorrelationId(), collector);
    }

    /**
     * Remove this collector from collector's map, if it is present. If map is empty (no more collectors await on consumer topic) unsubscribe from consumer.
     */
    public synchronized void unsubscribe(Collector collector) {
        collectorsByCorrelationId.remove(collector.getRequestMessage().getCorrelationId());

        if (collectorsByCorrelationId.isEmpty()) {
            channelManager.unsubscribe(topic);
        }
    }
}
