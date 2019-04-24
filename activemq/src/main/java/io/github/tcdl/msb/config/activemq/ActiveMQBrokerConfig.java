package io.github.tcdl.msb.config.activemq;

import com.typesafe.config.Config;
import io.github.tcdl.msb.api.SubscriptionType;
import io.github.tcdl.msb.config.ConfigurationUtil;

import java.util.Optional;

public class ActiveMQBrokerConfig {

    private String url;
    private final Optional<String> username;
    private final Optional<String> password;
    private final SubscriptionType defaultSubscriptionType;
    private final Optional<String> groupId;
    private final boolean durable;
    private final int prefetchCount;
    private final int connectionIdleTimeout;

    private ActiveMQBrokerConfig(String url, Optional<String> username, Optional<String> password,
                                 SubscriptionType defaultSubscriptionType, Optional<String> groupId,
                                 boolean durable, int prefetchCount, int connectionIdleTimeout) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.defaultSubscriptionType = defaultSubscriptionType;
        this.groupId = groupId;
        this.durable = durable;
        this.prefetchCount = prefetchCount;
        this.connectionIdleTimeout = connectionIdleTimeout;
    }

    public final static class ActiveMQBrokerConfigBuilder {

        private String url;
        private Optional<String> username;
        private Optional<String> password;
        private SubscriptionType defaultSubscriptionType;
        private Optional<String> groupId;
        private boolean durable;
        private int prefetchCount;
        private int connectionIdleTimeout;

        public ActiveMQBrokerConfigBuilder withConfig(Config config) {
            this.url = ConfigurationUtil.getString(config, "url");
            this.username = ConfigurationUtil.getOptionalString(config, "username");
            this.password = ConfigurationUtil.getOptionalString(config, "password");
            this.defaultSubscriptionType = SubscriptionType.valueOf(ConfigurationUtil.getString(config, "defaultSubscriptionType").toUpperCase());
            this.groupId = ConfigurationUtil.getOptionalString(config, "groupId");
            this.durable = ConfigurationUtil.getBoolean(config, "durable");
            this.prefetchCount = ConfigurationUtil.getInt(config, "prefetchCount");
            this.connectionIdleTimeout = ConfigurationUtil.getInt(config, "connectionIdleTimeout");
            return this;
        }

        public ActiveMQBrokerConfig build() {
            return new ActiveMQBrokerConfig(url, username, password, defaultSubscriptionType, groupId, durable, prefetchCount, connectionIdleTimeout);
        }
    }

    public String getUrl() {
        return url;
    }

    public Optional<String> getUsername() {
        return username;
    }

    public Optional<String> getPassword() {
        return password;
    }

    public SubscriptionType getDefaultSubscriptionType() {
        return defaultSubscriptionType;
    }

    public Optional<String> getGroupId() {
        return groupId;
    }

    public boolean isDurable() {
        return durable;
    }

    public int getPrefetchCount() {
        return prefetchCount;
    }

    public int getConnectionIdleTimeout() {
        return connectionIdleTimeout;
    }

    @Override
    public String toString() {
        return String.format("ActiveMQBrokerConfig [url=%s, username=%s, password=xxx, defaultSubscriptionType=%s, groupId=%s, durable=%s, prefetchCount=%s, connectionIdleTimeout=%s",
                url, username, defaultSubscriptionType, groupId, durable, prefetchCount, connectionIdleTimeout);
    }
}
