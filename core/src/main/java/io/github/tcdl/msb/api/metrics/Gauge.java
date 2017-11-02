package io.github.tcdl.msb.api.metrics;

/**
 * A gauge metric is an instantaneous reading of a particular value
 * @param <T> the type of the metric's value
 */

@FunctionalInterface
public interface Gauge<T> extends Metric {

    /**
     * @return the metric's current value
     */
    T getValue();
}