package io.github.tcdl.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

public class KafkaBrokerConfig {

    public final Logger log = LoggerFactory.getLogger(getClass());

    public static class KafkaBrokerConfigBuilder {

        public KafkaBrokerConfigBuilder(Config config) {
        }

        public KafkaBrokerConfig build() {
            return new KafkaBrokerConfig();
        }
    }
}
