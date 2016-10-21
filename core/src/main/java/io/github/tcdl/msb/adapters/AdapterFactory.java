package io.github.tcdl.msb.adapters;

import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.ResponderOptions;
import io.github.tcdl.msb.api.exception.ChannelException;
import io.github.tcdl.msb.api.exception.ConfigurationException;
import io.github.tcdl.msb.config.MsbConfig;

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
     * @deprecated use {@link AdapterFactory#createProducerAdapter(String, RequestOptions)}
     */
    @Deprecated
    default ProducerAdapter createProducerAdapter(String topic) {
        return createProducerAdapter(topic, RequestOptions.DEFAULTS);
    }

    ProducerAdapter createProducerAdapter(String topic, RequestOptions requestOptions);

    /**
     * @param topic topic name
     * @param isResponseTopic specify if this topic used to handle response
     * @return Consumer Adapter associated with a topic
     * @throws ChannelException if some problems during creation were occurred
     */
    ConsumerAdapter createConsumerAdapter(String topic, boolean isResponseTopic);

    /**
     * Creates ConsumerAdapter associated with a topic.
     * @param topic topic name
     * @throws ChannelException if a problems has occurred during creation
     */
    ConsumerAdapter createConsumerAdapter(String topic, ResponderOptions responderOptions, boolean isResponseTopic);

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
