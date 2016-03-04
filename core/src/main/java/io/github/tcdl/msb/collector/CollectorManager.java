package io.github.tcdl.msb.collector;

import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.MessageHandlerResolver;
import io.github.tcdl.msb.api.exception.ConsumerSubscriptionException;
import io.github.tcdl.msb.api.message.Message;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages instances of {@link Collector}s that listens to the same response topic.
 */
public class CollectorManager implements MessageHandlerResolver {

    private static final Logger LOG = LoggerFactory.getLogger(CollectorManager.class);
    private static final String LOGGING_NAME = "Collector manager";

    private volatile boolean isSubscribed = false;

    private final String topic;
    private final ChannelManager channelManager;
    Map<String, Collector> collectorsByCorrelationId = new ConcurrentHashMap<>();

    public CollectorManager(String topic, ChannelManager channelManager) {
        this.topic = topic;
        this.channelManager = channelManager;
    }

    /**
     * Determines correlationId from the incoming message and resolves the relevant {@link Collector} instance.
     */
    @Override public Optional<MessageHandler> resolveMessageHandler(Message message) {
        String correlationId = message.getCorrelationId();
        Collector collector = collectorsByCorrelationId.get(correlationId);
        if (collector != null) {
            return Optional.of(collector);
        } else {
            LOG.warn("Message with correlationId {} is not expected to be processed by any Collectors", correlationId);
            return Optional.empty();
        }
    }

    /**
     * @throws ConsumerSubscriptionException if another consumer already listen for messages on topic
     */
    public void registerCollector(Collector collector) {
        String correlationId = collector.getRequestMessage().getCorrelationId();
        collectorsByCorrelationId.putIfAbsent(correlationId, collector);

        if(!isSubscribed) {
            synchronized (this) {
                if (!isSubscribed) {
                    channelManager.subscribeForResponses(topic, this);
                    isSubscribed = true;
                }
            }
        }
    }

    /**
     * Remove this collector from collector's map, if it is present.
     */
    public void unregisterCollector(Collector collector) {
        collectorsByCorrelationId.remove(collector.getRequestMessage().getCorrelationId());
    }

    @Override
    public String getLoggingName() {
        return LOGGING_NAME;
    }
}
