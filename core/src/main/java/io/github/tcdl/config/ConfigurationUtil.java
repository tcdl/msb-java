package io.github.tcdl.config;

import io.github.tcdl.exception.ConfigurationException;

import java.util.Optional;

import com.typesafe.config.Config;

/**
 * {@link ConfigurationUtil} class provides a set of methods for managing configuration properties. *
 */
public class ConfigurationUtil {
    /**
     * @throws ConfigurationException if the path expression is invalid
     */
    public static boolean getBoolean(Config config, String key) {
        if (config.hasPath(key)) {
            return config.getBoolean(key);
        }
        throw new ConfigurationException(key);
    }

    public static boolean getBoolean(Config config, String key, boolean fallback) {
        if (config.hasPath(key)) {
            return config.getBoolean(key);
        }
        return fallback;
    }

    public static Optional<Boolean> getOptionalBoolean(Config config, String key) {
        if (config.hasPath(key)) {
            return Optional.of(config.getBoolean(key));
        }
        return Optional.empty();
    }

    public static String getString(Config config, String key, String fallback) {
        if (config.hasPath(key)) {
            return config.getString(key);
        }
        return fallback;
    }

    /**
     * @throws ConfigurationException if the path expression is invalid
     */
    public static String getString(Config config, String key) {
        if (config.hasPath(key)) {
            return config.getString(key);
        }
        throw new ConfigurationException(key);
    }

    public static Optional<String> getOptionalString(Config config, String key) {
        if (config.hasPath(key)) {
            return Optional.ofNullable(config.getString(key));
        }
        return Optional.empty();
    }

    public static int getInt(Config config, String key, int fallback) {
        if (config.hasPath(key)) {
            return config.getInt(key);
        }
        return fallback;
    }

    /**
     * @throws ConfigurationException if the path expression is invalid
     */
    public static int getInt(Config config, String key)  {
        if (config.hasPath(key)) {
            return config.getInt(key);
        }
        throw new ConfigurationException(key);
    }

}
