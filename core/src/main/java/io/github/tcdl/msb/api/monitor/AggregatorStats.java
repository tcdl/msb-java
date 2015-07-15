package io.github.tcdl.msb.api.monitor;

import io.github.tcdl.msb.config.ServiceDetails;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class represents internal state of {@link io.github.tcdl.msb.api.monitor.ChannelMonitorAggregator} that is passed to registered handlers.
 */
public class AggregatorStats {
    /**
     * Topics name -> info
     */
    private Map<String, AggregatorTopicStats> topicInfoMap = new ConcurrentHashMap<>();

    /**
     * Instance id -> info
     */
    private Map<String, ServiceDetails> serviceDetailsById = new ConcurrentHashMap<>();

    public Map<String, AggregatorTopicStats> getTopicInfoMap() {
        return topicInfoMap;
    }

    public Map<String, ServiceDetails> getServiceDetailsById() {
        return serviceDetailsById;
    }
}
