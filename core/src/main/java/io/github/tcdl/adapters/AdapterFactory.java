package io.github.tcdl.adapters;

import io.github.tcdl.adapters.mock.MockAdapterBuilder;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbConfigurations.BrokerAdapter;

/**
 * AdapterFactory creates an instance of Broker Adapter by means of Broker Adapter Builder.
 * Broker Adapter and Broker Adapter Builder are located in the separate proper JAR.
 */
public class AdapterFactory {

    public AdapterFactory() {
    }

    /**
     * 
     * @param brokerAdapterType
     * @return
     */
    private AdapterBuilder createAdapterBuilder(BrokerAdapter brokerAdapterType) {
        Object adapterBuilder;
        switch (brokerAdapterType)
        {
            case AMQP:
                try {
                    Class<?> clazz = Class.forName("io.github.tcdl.adapters.amqp.AmqpAdapterBuilder");
                    adapterBuilder = clazz.newInstance();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to create AMQP Adapter Builder", e);
                }
                break;
            case LOCAL:
                adapterBuilder = new MockAdapterBuilder();
                break;
            default:
                throw new RuntimeException("The required MSB Adapter is not supported");
        }

        if (!(adapterBuilder instanceof AdapterBuilder)) {
            throw new RuntimeException("Inconsistent Adapter Builder class");
        }
        return (AdapterBuilder) adapterBuilder;

    }

    public Adapter createAdapter(BrokerAdapter brokerAdapterType, String topic, MsbConfigurations msbConfig) {
        AdapterBuilder adapterBuilder = createAdapterBuilder(brokerAdapterType);
        adapterBuilder.setTopic(topic);
        adapterBuilder.setMsbConfigurations(msbConfig);
        return adapterBuilder.createAdapter();
    }
}
