package io.github.tcdl.adapters;

import io.github.tcdl.config.MsbConfigurations;

/**
 * MSBAdapterFactory interface represents a common way for creation a particular AdapterFactory
 * accordingly to MSB Configuration and associated with a proper Topic.
 * 
 */

public interface AdapterFactory {
    
    /**
     * Initialize AdapterFactory. The method should be called only once from AdapterFactoryLoader.
     * @param msbConfig - MsbConfigurations object 
     */
    void init(MsbConfigurations msbConfig);

    /**
     * Create Broker Adapter associated with a topic 
     * @param topic - topic name
     * @return
     */
    Adapter createAdapter(String topic);

    /**
     * Closes all resources used by amqp producers and consumers. Should be called for graceful shutdown.
     */
    void close();

}
