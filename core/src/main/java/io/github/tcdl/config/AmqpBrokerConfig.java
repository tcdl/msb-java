package io.github.tcdl.config;

import static io.github.tcdl.config.ConfigurationUtil.getString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

public class AmqpBrokerConfig {

    public final Logger log = LoggerFactory.getLogger(getClass());

    private final int port;
    private final String host;
    private String groupId;
    private final boolean durable;

    public AmqpBrokerConfig(String host, int port, String groupId, boolean durable) {
        this.port = port;
        this.host = host;
        this.groupId = groupId;
        this.durable = durable;
    }

    public static class AmqpBrokerConfigBuilder {
        private int port;
        private String host;
        private String groupId;
        private boolean durable;

        public AmqpBrokerConfigBuilder(Config config) {
            this.host = config.getString("host");
            this.port = config.getInt("port");
            this.groupId = getString(config, "groupId", null);
            this.durable = config.getBoolean("durable");
        }

        public AmqpBrokerConfig build() {
            return new AmqpBrokerConfig(host, port, groupId, durable);
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

    @Override
    public String toString() {
        return "AmqpBrokerConfig [host=" + host + ", port=" + port + ", groupId=" + groupId + ", durable=" + durable
                + "]";
    }

}