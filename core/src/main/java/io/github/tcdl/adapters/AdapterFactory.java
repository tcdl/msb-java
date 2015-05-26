package io.github.tcdl.adapters;

import io.github.tcdl.adapters.mock.AdapterBuilder;
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
    private MsbAdapterBuilder createAdapterBuilder(BrokerAdapter brokerAdapterType) {
        Object adapterBuilder;
        String className = "io.github.tcdl.adapters." + brokerAdapterType.toString() + ".AdapterBuilder";
        try {
            Class<?> clazz = Class.forName(className);
            adapterBuilder = clazz.newInstance();
        } catch(ClassNotFoundException e) {
            throw new RuntimeException("The required MSB Adapter '" + brokerAdapterType.toString() + "' is not supported.", e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Adapter Builder: " + className, e);
        }
 
        if (!(adapterBuilder instanceof MsbAdapterBuilder)) {
            throw new RuntimeException("Inconsistent Adapter Builder class: "  + className);
        }
        return (MsbAdapterBuilder) adapterBuilder;

    }

    public Adapter createAdapter(BrokerAdapter brokerAdapterType, String topic, MsbConfigurations msbConfig) {
        MsbAdapterBuilder adapterBuilder = createAdapterBuilder(brokerAdapterType);
        adapterBuilder.setTopic(topic);
        adapterBuilder.setMsbConfigurations(msbConfig);
        return adapterBuilder.createAdapter();
    }
}
