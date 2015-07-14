package io.github.tcdl.msb.config.amqp;

import java.nio.charset.Charset;
import java.util.Optional;

import io.github.tcdl.msb.config.ConfigurationUtil;
import io.github.tcdl.msb.api.exception.ConfigurationException;

import com.typesafe.config.Config;

public class AmqpBrokerConfig {

    private Charset charset;

    private final int port;
    private final String host;
    private Optional<String> username;
    private Optional<String> password;
    private Optional<String> virtualHost;

    private String groupId;
    private final boolean durable;
    private final int consumerThreadPoolSize;
    private final int consumerThreadPoolQueueCapacity;
    private final boolean requeueRejectedMessages;

    public AmqpBrokerConfig(Charset charset, String host, int port,
            Optional<String> username, Optional<String> password, Optional<String> virtualHost,
            String groupId, boolean durable, int consumerThreadPoolSize, int consumerThreadPoolQueueCapacity, boolean requeueRejectedMessages) {
        this.charset = charset;
        this.port = port;
        this.host = host;
        this.username = username;
        this.password = password;        
        this.virtualHost = virtualHost;
        this.groupId = groupId;
        this.durable = durable;
        this.consumerThreadPoolSize = consumerThreadPoolSize;
        this.consumerThreadPoolQueueCapacity = consumerThreadPoolQueueCapacity;
        this.requeueRejectedMessages = requeueRejectedMessages;
    }

    public static class AmqpBrokerConfigBuilder {
        private Charset charset;
        private int port;
        private String host;
        private Optional<String> username;
        private Optional<String> password;
        private Optional<String> virtualHost;
        private String groupId;
        private boolean durable;
        private int consumerThreadPoolSize;
        private int consumerThreadPoolQueueCapacity;
        private boolean requeueRejectedMessages;

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

            this.groupId = ConfigurationUtil.getString(config, "groupId");
            this.durable = ConfigurationUtil.getBoolean(config, "durable");
            this.consumerThreadPoolSize = ConfigurationUtil.getInt(config, "consumerThreadPoolSize");
            this.consumerThreadPoolQueueCapacity = ConfigurationUtil.getInt(config, "consumerThreadPoolQueueCapacity");
            this.requeueRejectedMessages = ConfigurationUtil.getBoolean(config, "requeueRejectedMessages");
            return this;
        }

        /**
         * @throws ConfigurationException if provided configuration is broken
         */
        public AmqpBrokerConfig build() {
            return new AmqpBrokerConfig(charset, host, port, username, password, virtualHost,
                    groupId, durable, consumerThreadPoolSize, consumerThreadPoolQueueCapacity, requeueRejectedMessages);
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

    public String getGroupId() {
        return groupId;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public int getConsumerThreadPoolSize() {
        return consumerThreadPoolSize;
    }

    public int getConsumerThreadPoolQueueCapacity() {
        return consumerThreadPoolQueueCapacity;
    }

    public boolean isRequeueRejectedMessages() {
        return requeueRejectedMessages;
    }

    @Override
    public String toString() {
        return String.format("AmqpBrokerConfig [charset=%s, host=%s, port=%d, username=%s, password=xxx, virtualHost=%s, groupId=%s, durable=%s, consumerThreadPoolSize=%s, consumerThreadPoolQueueCapacity=%s, requeueRejectedMessages=%s]",
                charset, host, port, username, virtualHost, groupId, durable, consumerThreadPoolSize, consumerThreadPoolQueueCapacity, requeueRejectedMessages);
    }

}