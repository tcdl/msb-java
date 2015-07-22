package io.github.tcdl.msb.api.monitor;

import io.github.tcdl.msb.config.ServiceDetails;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents aggregated statistics collected from multiple instances of microservices
 * (that is transmitted over the bus by from their {@link io.github.tcdl.msb.monitor.agent.ChannelMonitorAgent}s).
 */
public class AggregatorStats {
    /**
     * Collects information about all topics that we know of.
     *
     * Structure of the map: topics name -> stats
     */
    private Map<String, AggregatorTopicStats> topicInfoMap = new ConcurrentHashMap<>();

    /**
     * Collects information about all instances of microservices that we know of.
     *
     * Structure of the map: service instance id -> details
     */
    private Map<String, ServiceDetails> serviceDetailsById = new ConcurrentHashMap<>();

    public Map<String, AggregatorTopicStats> getTopicInfoMap() {
        return topicInfoMap;
    }

    public Map<String, ServiceDetails> getServiceDetailsById() {
        return serviceDetailsById;
    }
}
