package tcdl.msb.config;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

public class ConfigurationUtil {

    public static boolean getBoolean(Config config, String key, boolean fallback) {
        if(config.hasPath(key)) return config.getBoolean(key);
        return fallback;
    }

    public static Optional<Boolean> getOptionalBoolean(Config config, String key) {
        if(config.hasPath(key)) return Optional.of(config.getBoolean(key));
        return Optional.absent();
    }

    public static String getString(Config config, String key, String fallback) {
        if(config.hasPath(key)) return config.getString(key);
        return fallback;
    }

    public static Optional<String> getOptionalString(Config config, String key) {
        if(config.hasPath(key)) return Optional.of(config.getString(key));
        return Optional.absent();
    }

    public static int getInt(Config config, String key, int fallback) {
        if(config.hasPath(key)) return config.getInt(key);
        return fallback;
    }

}
