package io.github.tcdl.collector;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.github.tcdl.ChannelManager;

public class CollectorManagerFactory {

    private Map<String, CollectorManager> collectorManagersByTopic;
    private ChannelManager channelManager;

    public CollectorManagerFactory(ChannelManager channelManager) {
        this.collectorManagersByTopic = new ConcurrentHashMap<>();
        this.channelManager = channelManager;
    }

    public CollectorManager findOrCreateCollectorManager(final String topic) {
        CollectorManager collectorManager = collectorManagersByTopic.computeIfAbsent(topic, key -> {
            CollectorManager newCollectorManager =  new CollectorManager(topic, channelManager);
            channelManager.subscribe(topic, newCollectorManager);
            return newCollectorManager;
        });

        return collectorManager;
    }
}
