package io.github.tcdl.collector;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.github.tcdl.ChannelManager;
import org.apache.commons.lang3.Validate;

public class CollectorManagerFactory {

    private Map<String, CollectorManager> collectorManagersByTopic;
    private ChannelManager channelManager;

    public CollectorManagerFactory(ChannelManager channelManager) {
        this.collectorManagersByTopic = new ConcurrentHashMap<>();
        this.channelManager = channelManager;
    }

    public CollectorManager findOrCreateCollectorManager(final String topic) {
        Validate.notNull(topic, "field 'topic' is null");
        CollectorManager collectorManager = collectorManagersByTopic.computeIfAbsent(topic, key -> {
            CollectorManager newCollectorManager =  new CollectorManager(topic, channelManager);
            channelManager.subscribe(topic, newCollectorManager);
            return newCollectorManager;
        });

        return collectorManager;
    }
}
