package io.github.tcdl.msb.config;

import io.github.tcdl.msb.api.exception.ConfigurationException;

import java.util.Optional;

import com.typesafe.config.Config;

/**
 * {@link ConfigurationUtil} class provides a set of static methods for managing configuration properties.
 */
public class ConfigurationUtil {

    /**
     * Get mandatory Boolean value from Config for a specified key  
     * @param config is Config
     * @param key is a specified key
     * @return boolean
     * @throws ConfigurationException if specified key is not present in the Config
     */
    public static boolean getBoolean(Config config, String key) {
        if (config.hasPath(key)) {
            return config.getBoolean(key);
        }
        throw new ConfigurationException(key);
    }

    /**
     * Get optional Boolean value from Config for a specified key.
     * Return empty Optional is the key is not present  
     * @param config configuration for MSB library
     * @param key specified key in configuration
     * @return optional that represents the Boolean value
     */
    public static Optional<Boolean> getOptionalBoolean(Config config, String key) {
        if (config.hasPath(key)) {
            return Optional.of(config.getBoolean(key));
        }
        return Optional.empty();
    }

    /**
     * Get mandatory String value from Config for a specified key  
     * @param config configuration for MSB library
     * @param key specified key in configuration
     * @return value for specified key
     * @throws ConfigurationException if specified key is not present in the Config
     */
    public static String getString(Config config, String key) {
        if (config.hasPath(key)) {
            return config.getString(key);
        }
        throw new ConfigurationException(key);
    }

    /**
     * Get optional String value from Config for a specified key. 
     * Return empty Optional is the key is not present. 
     * @param config configuration for MSB library
     * @param key specified key in configuration
     * @return optional that represents the Boolean value
     */
    public static Optional<String> getOptionalString(Config config, String key) {
        if (config.hasPath(key)) {
            return Optional.ofNullable(config.getString(key));
        }
        return Optional.empty();
    }

    /**
     * Get mandatory int value from Config for a specified key  
     * @param config configuration for MSB library
     * @param key specified key in configuration
     * @return value for specified key
     * @throws ConfigurationException if specified key is not present in the configuration
     */
    public static int getInt(Config config, String key) {
        if (config.hasPath(key)) {
            return config.getInt(key);
        }
        throw new ConfigurationException(key);
    }

    /**
     * Get mandatory long value from Config for a specified key
     * @param config configuration for MSB library
     * @param key specified key in configuration
     * @return value for specified key
     * @throws ConfigurationException if specified key is not present in the configuration
     */
    public static long getLong(Config config, String key) {
        if (config.hasPath(key)) {
            return config.getLong(key);
        }
        throw new ConfigurationException(key);
    }
}
