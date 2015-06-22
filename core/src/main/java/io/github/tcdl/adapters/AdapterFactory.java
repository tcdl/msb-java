package io.github.tcdl.adapters;

import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.exception.ConfigurationException;

/**
 * MSBAdapterFactory interface represents a common way for creation a particular AdapterFactory
 * accordingly to MSB Configuration and associated with a proper Topic.
 */
public interface AdapterFactory {
    
    /**
     * Initialize AdapterFactory. The method should be called only once from AdapterFactoryLoader.
     * @param msbConfig - MsbConfigurations object 
     * @throws ConfigurationException if provided configuration is broken
     */
    void init(MsbConfigurations msbConfig);

    /**
     * @param topic - topic name
     * @return producer adapter associated with a topic
     */
    ProducerAdapter createProducerAdapter(String topic);

    ConsumerAdapter createConsumerAdapter(String topic);

    /**
     * Closes all resources used by amqp producers and consumers. Should be called for graceful shutdown.
     */
    void close();

}
