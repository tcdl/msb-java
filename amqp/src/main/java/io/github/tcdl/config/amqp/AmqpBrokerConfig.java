package io.github.tcdl.config.amqp;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.github.tcdl.config.ConfigurationUtil.getString;

public class AmqpBrokerConfig {

    private final int port;
    private final String host;
    private String groupId;
    private final boolean durable;
    private final int consumerThreadPoolSize;

    public AmqpBrokerConfig(String host, int port, String groupId, boolean durable, int consumerThreadPoolSize) {
        this.port = port;
        this.host = host;
        this.groupId = groupId;
        this.durable = durable;
        this.consumerThreadPoolSize = consumerThreadPoolSize;
    }

    public static class AmqpBrokerConfigBuilder {
        private int port;
        private String host;
        private String groupId;
        private boolean durable;
        private int consumerThreadPoolSize;

        public AmqpBrokerConfigBuilder(Config config) {
            this.host = config.getString("host");
            this.port = config.getInt("port");
            this.groupId = getString(config, "groupId", null);
            this.durable = config.getBoolean("durable");
            this.consumerThreadPoolSize = config.getInt("consumerThreadPoolSize");
       }

        public AmqpBrokerConfig build() {
            return new AmqpBrokerConfig(host, port, groupId, durable, consumerThreadPoolSize);
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
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
        return String.format("AmqpBrokerConfig [host=%s, port=%d, groupId=%s, durable=%s, consumerThreadPoolSize=%s]", host, port, groupId, durable, consumerThreadPoolSize);
    }

}