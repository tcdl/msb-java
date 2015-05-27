package io.github.tcdl.adapters;

import java.lang.reflect.Constructor;

import io.github.tcdl.config.MsbConfigurations;

/**
 * AdapterFactory creates an instance of Broker Adapter by means of Broker Adapter Builder.
 * Broker Adapter and Broker Adapter Builder are located in the separate proper JAR.
 */
public class AdapterFactoryLoader {
    
    private final MsbConfigurations msbConfig;
    
    public AdapterFactoryLoader(MsbConfigurations msbConfig) {
        this.msbConfig = msbConfig;
    }

    /**
     * 
     * @param brokerAdapterType
     * @return
     */
    public MsbAdapterFactory getAdapterFactory() {
        Object adapterFactory;
        String className = "io.github.tcdl.adapters." + msbConfig.getBrokerType().toString() + ".AdapterFactory";
        try {
            Class<?> clazz = Class.forName(className);
            Constructor<?> constructor = clazz.getConstructor(MsbConfigurations.class);
            adapterFactory = constructor.newInstance(msbConfig);
        } catch(ClassNotFoundException e) {
            throw new RuntimeException("The required MSB Adapter '" + msbConfig.getBrokerType().toString() + "' is not supported.", e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Adapter Builder: " + className, e);
        }
 
        if (!(adapterFactory instanceof MsbAdapterFactory)) {
            throw new RuntimeException("Inconsistent Adapter Builder class: "  + className);
        }
        return (MsbAdapterFactory) adapterFactory;

    }

}
