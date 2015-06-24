package io.github.tcdl.adapters;

import io.github.tcdl.exception.AdapterInitializationException;

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
     * @throws AdapterInitializationException if some problems during creation of {@link AdapterFactory} object were occurred
     * @return {@link AdapterFactory}
     */
    public AdapterFactory getAdapterFactory() {
        AdapterFactory adapterFactory;
        String adapterFactoryClassName = msbConfig.getBrokerAdapterFactory();
        try {
            Class clazz = Class.forName(adapterFactoryClassName);
            adapterFactory = (AdapterFactory) clazz.newInstance();
        } catch (ClassNotFoundException e) {
            throw new AdapterInitializationException("The required MSB Adapter Factory '" + adapterFactoryClassName + "' is not supported.", e);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new AdapterInitializationException("Failed to create Adapter Factory: " + adapterFactoryClassName, e);
        } catch (ClassCastException e) {
            throw new AdapterInitializationException("Inconsistent Adapter Factory class: " + adapterFactoryClassName);
        }

        adapterFactory.init(msbConfig);

        return adapterFactory;
    }
}
