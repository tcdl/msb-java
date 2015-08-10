package io.github.tcdl.msb.api.monitor;

import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.ObjectFactory;

/**
 * Gathers statistics over the bus from other running microservices that have {@link io.github.tcdl.msb.monitor.agent.ChannelMonitorAgent} activated via
 * {@link io.github.tcdl.msb.api.MsbContextBuilder#enableChannelMonitorAgent(boolean)}. The statistics is taken from 2 sources:
 *
 * 1. By listening to {@link io.github.tcdl.msb.support.Utils#TOPIC_ANNOUNCE}
 * 2. By sending periodic heartbeats to {@link io.github.tcdl.msb.support.Utils#TOPIC_HEARTBEAT} and analysing responses. This responses will be aggregated and
 * then overwrite stats with most recent information to detect that some microservices went down.
 *
 * Typical lifecycle for this aggregator is:
 * 1. Create instance via {@link ObjectFactory#createChannelMonitorAggregator(Callback)}
 * 2. Activate the aggregator via {@link #start()}
 * 3. The registered handler processes the stats from the bus
 * 4. Deactivate the aggregator via {@link #stop()}
 */
public interface ChannelMonitorAggregator {

    /**
     * See {@link #start(boolean, long, int)}
     */
    long DEFAULT_HEARTBEAT_INTERVAL_MS = 10000;

    /**
     * See {@link #start(boolean, long, int)}
     */
    int DEFAULT_HEARTBEAT_TIMEOUT_MS = 5000;

    /**
     * Convenience method that activates the aggregator with default heartbeat parameters
     */
    default void start() {
        start(true, DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_HEARTBEAT_TIMEOUT_MS);
    }

    /**
     * Activates the aggregator with the given parameters
     * @param activateHeartbeats if equal to <code>false</code> then no periodic heartbeats are sent and stats is obtained only from announcement channel
     * @param heartbeatIntervalMs Interval in milliseconds between heartbeat requests
     * @param heartbeatTimeoutMs how long does the aggregator waits in milliseconds for responses after each heartbeat
     */
    void start(boolean activateHeartbeats, long heartbeatIntervalMs, int heartbeatTimeoutMs);

    /**
     * Deactivates this aggregator. After this method is invoked the object is not usable. This method can be invoked multiple times.
     */
    void stop();
}
