package io.github.tcdl.msb.adapters;

import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.api.exception.ChannelException;
import io.github.tcdl.msb.api.exception.ConfigurationException;

/**
 * MSBAdapterFactory interface represents a common way for creation a particular AdapterFactory
 * accordingly to MSB Configuration and associated with a proper Topic.
 */
public interface AdapterFactory {

    /**
     * Initialize AdapterFactory. The method should be called only once from AdapterFactoryLoader.
     *
     * @param msbConfig {@link MsbConfig} object
     * @throws ConfigurationException if provided configuration is broken
     */
    void init(MsbConfig msbConfig);

    /**
     * @param topic topic name
     * @return Producer Adapter associated with a topic
     * @throws ChannelException if some problems during creation were occurred
     */
    ProducerAdapter createProducerAdapter(String topic);

    /**
     * @param topic topic name
     * @param isResponseTopic specify if this topic used to handle response
     * @return Consumer Adapter associated with a topic
     * @throws ChannelException if some problems during creation were occurred
     */
    ConsumerAdapter createConsumerAdapter(String topic, boolean isResponseTopic);

    /**
     * @return true if custom MSB threading model should be used.
     * @return false if {@link io.github.tcdl.msb.MessageHandler} should be invoked directly.
     */
    boolean isUseMsbThreadingModel();

    /**
     * Closes all resources used by amqp producers and consumers. Should be called for graceful shutdown.
     */
    void shutdown();
}
