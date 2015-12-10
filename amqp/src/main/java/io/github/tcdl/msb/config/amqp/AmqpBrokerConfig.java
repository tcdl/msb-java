package io.github.tcdl.msb.config.amqp;

import io.github.tcdl.msb.api.exception.ConfigurationException;
import io.github.tcdl.msb.config.ConfigurationUtil;

import java.nio.charset.Charset;
import java.util.Optional;

import com.typesafe.config.Config;

public class AmqpBrokerConfig {

    private Charset charset;

    private final int port;
    private final String host;
    private Optional<String> username;
    private Optional<String> password;
    private Optional<String> virtualHost;
    private boolean useSSL;

    private Optional<String> groupId;
    private final boolean durable;
    private final int consumerThreadPoolSize;
    private final int consumerThreadPoolQueueCapacity;
    private final int heartbeatIntervalSec;
    private final long networkRecoveryIntervalMs;
    private final int prefetchCount;

    public AmqpBrokerConfig(Charset charset, String host, int port,
            Optional<String> username, Optional<String> password, Optional<String> virtualHost, boolean useSSL,
            Optional<String> groupId, boolean durable, int consumerThreadPoolSize, int consumerThreadPoolQueueCapacity,
            int heartbeatIntervalSec, long networkRecoveryIntervalMs, int prefetchCount) {
        this.charset = charset;
        this.port = port;
        this.host = host;
        this.username = username;
        this.password = password;        
        this.virtualHost = virtualHost;
        this.useSSL = useSSL;
        this.groupId = groupId;
        this.durable = durable;
        this.consumerThreadPoolSize = consumerThreadPoolSize;
        this.consumerThreadPoolQueueCapacity = consumerThreadPoolQueueCapacity;
        this.heartbeatIntervalSec = heartbeatIntervalSec;
        this.networkRecoveryIntervalMs = networkRecoveryIntervalMs;
        this.prefetchCount = prefetchCount;
    }

    public static class AmqpBrokerConfigBuilder {
        private Charset charset;
        private int port;
        private String host;
        private Optional<String> username;
        private Optional<String> password;
        private Optional<String> virtualHost;
        private boolean useSSL;
        private Optional<String> groupId;
        private boolean durable;
        private int consumerThreadPoolSize;
        private int consumerThreadPoolQueueCapacity;
        private int heartbeatIntervalSec;
        private long networkRecoveryIntervalMs;
        private int prefetchCount;

        /**
         * Initialize Builder with Config
         * @param config is a row broker configuration
         * @throws ConfigurationException if provided configuration is broken
         */
        public AmqpBrokerConfigBuilder withConfig(Config config) {
            String charsetName = ConfigurationUtil.getString(config, "charsetName");
            try {
                this.charset = Charset.forName(charsetName);
            } catch (Exception e) {
                throw new ConfigurationException(String.format("Unable to load the configured charset: '%s'", charsetName), e);
            }

            this.host = ConfigurationUtil.getString(config, "host");
            this.port = ConfigurationUtil.getInt(config, "port");

            this.username = ConfigurationUtil.getOptionalString(config, "username");
            this.password = ConfigurationUtil.getOptionalString(config, "password");
            this.virtualHost = ConfigurationUtil.getOptionalString(config, "virtualHost");
            this.useSSL = ConfigurationUtil.getBoolean(config, "useSSL");

            this.groupId = ConfigurationUtil.getOptionalString(config, "groupId");
            this.durable = ConfigurationUtil.getBoolean(config, "durable");
            this.consumerThreadPoolSize = ConfigurationUtil.getInt(config, "consumerThreadPoolSize");
            this.consumerThreadPoolQueueCapacity = ConfigurationUtil.getInt(config, "consumerThreadPoolQueueCapacity");
            this.heartbeatIntervalSec = ConfigurationUtil.getInt(config, "heartbeatIntervalSec");
            this.networkRecoveryIntervalMs = ConfigurationUtil.getLong(config, "networkRecoveryIntervalMs");
            this.prefetchCount = ConfigurationUtil.getInt(config, "prefetchCount");
            return this;
        }

        /**
         * @throws ConfigurationException if provided configuration is broken
         */
        public AmqpBrokerConfig build() {
            return new AmqpBrokerConfig(charset, host, port, username, password, virtualHost, useSSL,
                    groupId, durable, consumerThreadPoolSize, consumerThreadPoolQueueCapacity, 
                    heartbeatIntervalSec, networkRecoveryIntervalMs, prefetchCount);
        }
    }

    public Charset getCharset() {
        return charset;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public Optional<String> getUsername() {
        return username;
    }

    public Optional<String> getPassword() {
        return password;
    }

    public Optional<String> getVirtualHost() {
        return virtualHost;
    }

    public boolean useSSL() {
        return useSSL;
    }

    public Optional<String> getGroupId() {
        return groupId;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setGroupId(Optional<String> groupId) {
        this.groupId = groupId;
    }

    public int getConsumerThreadPoolSize() {
        return consumerThreadPoolSize;
    }

    public int getConsumerThreadPoolQueueCapacity() {
        return consumerThreadPoolQueueCapacity;
    }

    public int getHeartbeatIntervalSec() {
        return heartbeatIntervalSec;
    }

    public long getNetworkRecoveryIntervalMs() {
        return networkRecoveryIntervalMs;
    }

    public int getPrefetchCount() {
        return prefetchCount;
    }
    
    @Override
    public String toString() {
        return String.format("AmqpBrokerConfig [charset=%s, host=%s, port=%d, username=%s, password=xxx, virtualHost=%s, useSSL=%s, groupId=%s, durable=%s, "
                        + "consumerThreadPoolSize=%s, consumerThreadPoolQueueCapacity=%s, heartbeatIntervalSec=%s, "
                        + "networkRecoveryIntervalMs=%s, prefetchCount=%s]",
                charset, host, port, username, virtualHost, useSSL, groupId, durable, consumerThreadPoolSize, consumerThreadPoolQueueCapacity, 
                heartbeatIntervalSec, networkRecoveryIntervalMs, prefetchCount);
    }

}