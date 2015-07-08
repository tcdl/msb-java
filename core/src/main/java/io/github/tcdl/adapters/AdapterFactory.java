package io.github.tcdl.adapters;

import io.github.tcdl.config.MsbConfig;
import io.github.tcdl.api.exception.ChannelException;
import io.github.tcdl.api.exception.ConfigurationException;

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
    void init(MsbConfig msbConfig);

    /**
     * @param topic - topic name
     * @return Producer Adapter associated with a topic
     * @throws ChannelException if some problems during creation were occurred
     */
    ProducerAdapter createProducerAdapter(String topic);

    /**
     * @param topic - topic name
     * @return Consumer Adapter associated with a topic
     * @throws ChannelException if some problems during creation were occurred
     */
    ConsumerAdapter createConsumerAdapter(String topic);

    /**
     * Closes all resources used by amqp producers and consumers. Should be called for graceful shutdown.
     */
    void shutdown();
}
