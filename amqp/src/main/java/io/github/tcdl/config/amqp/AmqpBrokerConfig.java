package io.github.tcdl.config.amqp;

import java.util.Optional;

import io.github.tcdl.config.ConfigurationUtil;

import com.typesafe.config.Config;

import static io.github.tcdl.config.ConfigurationUtil.getString;

public class AmqpBrokerConfig {
    
    public static final String HOST_DEFAULT = "127.0.0.1";
    public static final int PORT_DEFAULT = 5672;
    public static final String GROUP_ID_DEFAULT = "msb-java";
    public static final boolean DURABLE_DEFAULT = false;
    public static final int CONSUMER_THREAD_POOL_SIZE_DEFAULT = 5;
    
    private final int port;
    private final String host;
    private Optional<String> userName;
    private Optional<String> password;
    private Optional<String> virtualHost;

    private String groupId;
    private final boolean durable;
    private final int consumerThreadPoolSize;

    public AmqpBrokerConfig(String host, int port, 
            Optional<String> userName, Optional<String> password, Optional<String> virtualHost,
            String groupId, boolean durable, int consumerThreadPoolSize) {
        this.port = port;
        this.host = host;
        this.userName = userName;
        this.password = password;        
        this.virtualHost = virtualHost;
        this.groupId = groupId;
        this.durable = durable;
        this.consumerThreadPoolSize = consumerThreadPoolSize;
    }

    public static class AmqpBrokerConfigBuilder {
        private int port;
        private String host;
        private Optional<String> userName;
        private Optional<String> password;
        private Optional<String> virtualHost;
        private String groupId;
        private boolean durable;
        private int consumerThreadPoolSize;

        public AmqpBrokerConfigBuilder(Config config) {
            
            this.host = ConfigurationUtil.getString(config, "host", HOST_DEFAULT);
            this.port = ConfigurationUtil.getInt(config, "port", PORT_DEFAULT);

            this.userName = ConfigurationUtil.getOptionalString(config, "userName");
            this.password = ConfigurationUtil.getOptionalString(config, "password");
            this.virtualHost = ConfigurationUtil.getOptionalString(config, "virtualHost");
            
            this.groupId = ConfigurationUtil.getString(config, "groupId", GROUP_ID_DEFAULT);
            this.durable = ConfigurationUtil.getBoolean(config, "durable", DURABLE_DEFAULT);
            this.consumerThreadPoolSize = ConfigurationUtil.getInt(config, "consumerThreadPoolSize", CONSUMER_THREAD_POOL_SIZE_DEFAULT);
       }

        public AmqpBrokerConfig build() {
            return new AmqpBrokerConfig(host, port, userName, password, virtualHost, 
                    groupId, durable, consumerThreadPoolSize);
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public Optional<String> getUserName() {
        return userName;
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

    @Override
    public String toString() {
        return String.format("AmqpBrokerConfig [host=%s, port=%d, userName=%s, password=%s, virtualHost=%s, groupId=%s, durable=%s, consumerThreadPoolSize=%s]", 
                host, port, userName, password, virtualHost, groupId, durable, consumerThreadPoolSize);
    }

}