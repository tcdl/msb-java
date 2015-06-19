package io.github.tcdl.adapters;

import io.github.tcdl.exception.MsbException;
import org.apache.commons.lang3.StringUtils;

import io.github.tcdl.adapters.mock.MockAdapterFactory;
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

    public AdapterFactory getAdapterFactory() {
        Object adapterFactory;
        String adapterFactoryClassName = msbConfig.getBrokerAdapterFactory();
        if (!StringUtils.isEmpty(adapterFactoryClassName)) {
            try {
                Class clazz = Class.forName(adapterFactoryClassName);
                adapterFactory = clazz.newInstance();
            } catch (ClassNotFoundException e) {
                throw new MsbException("The required MSB Adapter Factory '" + adapterFactoryClassName + "' is not supported.", e);
            } catch (ReflectiveOperationException e) {
                throw new MsbException("Failed to create Adapter Factory: " + adapterFactoryClassName, e);
            }

            if (!(adapterFactory instanceof AdapterFactory)) {
                throw new MsbException("Inconsistent Adapter Factory class: " + adapterFactoryClassName);
            }
        } else {
            adapterFactory = new MockAdapterFactory();
        }

        if (!(adapterFactory instanceof AdapterFactory)) {
            throw new RuntimeException("Inconsistent Adapter Factory class: " + adapterFactoryClassName);
        }

        ((AdapterFactory) adapterFactory).init(msbConfig);
        return (AdapterFactory) adapterFactory;
    }

}
