package io.github.tcdl.msb.api.metrics;

import java.util.Map;

/**
 * A set of named metrics.
 */
public interface MetricSet extends Metric {

    /**
     * {@value #MESSAGE_COUNT_METRIC} metric key for the number available messages as {@link Gauge} of {@link Long} type
     */
    String MESSAGE_COUNT_METRIC = "availableMessageCount";

    /**
     * {@value #CONSUMER_CONNECTED_METRIC} metric key for the consumer status {@link Gauge} of {@link Long} type
     */
    String CONSUMER_CONNECTED_METRIC = "consumerConnected";

    /**
     * @return supported metric by name
     */
    default Metric getMetric(String metricName) {
        return getMetrics().get(metricName);
    }

    /**
     * A map of metric names to metrics.
     *
     * @return the metrics
     */
    Map<String, Metric> getMetrics();

}