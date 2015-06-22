package io.github.tcdl.adapters;

import io.github.tcdl.exception.AdapterFactoryNotSupportedException;
import io.github.tcdl.exception.FailedToCreateAdapterFactoryException;
import io.github.tcdl.exception.InconsistentAdapterFactoryException;

import io.github.tcdl.config.MsbConfigurations;

/**
 * AdapterFactory creates an instance of Broker Adapter by means of Broker AdapterFactory.
 * Broker AdapterFactory and Broker Adapter are located in the separate proper JAR.
 */
public class AdapterFactoryLoader {

    private final MsbConfigurations msbConfig;

    public AdapterFactoryLoader(MsbConfigurations msbConfig) {
        this.msbConfig = msbConfig;
    }

    /**
     * @throws AdapterFactoryNotSupportedException if not supported Adapter Factory class
     * @throws FailedToCreateAdapterFactoryException if some problems during creation Adapter Factory object were occurred
     * @throws InconsistentAdapterFactoryException if inconsistent Adapter Factory class
     */
    public AdapterFactory getAdapterFactory() {
        Object adapterFactory;
        String adapterFactoryClassName = msbConfig.getBrokerAdapterFactory();
        try {
            Class clazz = Class.forName(adapterFactoryClassName);
            adapterFactory = clazz.newInstance();
        } catch (ClassNotFoundException e) {
            throw new AdapterFactoryNotSupportedException("The required MSB Adapter Factory '" + adapterFactoryClassName + "' is not supported.", e);
        } catch (Exception e) {
            throw new FailedToCreateAdapterFactoryException("Failed to create Adapter Factory: " + adapterFactoryClassName, e);
        }

        if (!(adapterFactory instanceof AdapterFactory)) {
            throw new InconsistentAdapterFactoryException("Inconsistent Adapter Factory class: " + adapterFactoryClassName);
        }

        ((AdapterFactory) adapterFactory).init(msbConfig);
        return (AdapterFactory) adapterFactory;
    }

}
