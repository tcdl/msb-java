package io.github.tcdl.adapters;

/**
 * AdapterFactory creates an instance of Broker Adapter accordingly to MSB Configuration.
 * Represented as a Singleton.
 */
public interface AdapterFactory {
    Adapter createAdapter(String topic);
}
