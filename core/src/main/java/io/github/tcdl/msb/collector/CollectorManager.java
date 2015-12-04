package io.github.tcdl.msb.collector;

import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.api.exception.ConsumerSubscriptionException;
import io.github.tcdl.msb.api.message.Message;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages instances of {@link Collector}s that listens to the same response topic.
 */
public class CollectorManager implements MessageHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CollectorManager.class);

    private boolean isSubscribed = false;

    private String topic;
    private ChannelManager channelManager;
    Map<String, Collector> collectorsByCorrelationId = new ConcurrentHashMap<>();

    public CollectorManager(String topic, ChannelManager channelManager) {
        this.topic = topic;
        this.channelManager = channelManager;
    }

    /**
     * Determines correlationId from the incoming message and invokes the relevant {@link Collector} instance.
     */
    @Override
    public void handleMessage(Message message, ConsumerAdapter.AcknowledgementHandler acknowledgeHandler) {
        String correlationId = message.getCorrelationId();
        Collector collector = collectorsByCorrelationId.get(correlationId);
        if (collector != null) {
            collector.handleMessage(message, acknowledgeHandler);
        } else {
            LOG.warn("Message with correlationId {} is not expected to be processed by any Collectors", correlationId);
        }

    }

    /**
     * @throws ConsumerSubscriptionException if another consumer already listen for messages on topic
     */
    public void registerCollector(Collector collector) {
        String correlationId = collector.getRequestMessage().getCorrelationId();
        collectorsByCorrelationId.putIfAbsent(correlationId, collector);

        synchronized (this) {
            if (!isSubscribed) {
                channelManager.subscribe(topic, this);
                isSubscribed = true;
            }
        }
    }

    /**
     * Remove this collector from collector's map, if it is present. If map is empty (no more collectors await on consumer topic) unsubscribe from consumer.
     */
    public void unregisterCollector(Collector collector) {
        collectorsByCorrelationId.remove(collector.getRequestMessage().getCorrelationId());

        synchronized (this) {
            if (collectorsByCorrelationId.isEmpty() && isSubscribed) {
                channelManager.unsubscribe(topic);
                isSubscribed = false;
            }
        }
    }
}
