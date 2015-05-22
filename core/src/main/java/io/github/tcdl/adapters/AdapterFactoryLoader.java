package io.github.tcdl.adapters;

import com.typesafe.config.Config;
import io.github.tcdl.exception.AdapterConfigurationException;

import java.lang.reflect.Constructor;

public class AdapterFactoryLoader {

    private final Config config;

    public AdapterFactoryLoader(Config config) {
        this.config = config;
    }

    public AdapterFactory getAdapterFactory() {
        String className = config.getString("msbConfig.brokerAdapterFactory");
        try {
            Class<?> clazz = Class.forName(className);
            Constructor<?> constructor = clazz.getConstructor(Config.class);
            Object adapterFactory = constructor.newInstance(config);
            return (AdapterFactory) adapterFactory;
        } catch (Exception e) {
            throw new AdapterConfigurationException(e);
        }
    }
}
