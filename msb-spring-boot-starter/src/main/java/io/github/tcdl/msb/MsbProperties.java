package io.github.tcdl.msb;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.nio.charset.Charset;

@ConfigurationProperties("msbConfig")
public class MsbProperties {

    ServiceDetails serviceDetails = new ServiceDetails();
    String brokerAdapterFactory;
    Integer timerThreadPoolSize;
    Boolean validateMessage;
    Integer consumerThreadPoolSize;
    Integer consumerThreadPoolQueueCapacity;
    BrokerConfig brokerConfig = new BrokerConfig();

    public ServiceDetails getServiceDetails() {
        return serviceDetails;
    }

    public void setServiceDetails(ServiceDetails serviceDetails) {
        this.serviceDetails = serviceDetails;
    }

    public String getBrokerAdapterFactory() {
        return brokerAdapterFactory;
    }

    public void setBrokerAdapterFactory(String brokerAdapterFactory) {
        this.brokerAdapterFactory = brokerAdapterFactory;
    }

    public Integer getTimerThreadPoolSize() {
        return timerThreadPoolSize;
    }

    public void setTimerThreadPoolSize(Integer timerThreadPoolSize) {
        this.timerThreadPoolSize = timerThreadPoolSize;
    }

    public Boolean getValidateMessage() {
        return validateMessage;
    }

    public void setValidateMessage(Boolean validateMessage) {
        this.validateMessage = validateMessage;
    }

    public Integer getConsumerThreadPoolSize() {
        return consumerThreadPoolSize;
    }

    public void setConsumerThreadPoolSize(Integer consumerThreadPoolSize) {
        this.consumerThreadPoolSize = consumerThreadPoolSize;
    }

    public Integer getConsumerThreadPoolQueueCapacity() {
        return consumerThreadPoolQueueCapacity;
    }

    public void setConsumerThreadPoolQueueCapacity(Integer consumerThreadPoolQueueCapacity) {
        this.consumerThreadPoolQueueCapacity = consumerThreadPoolQueueCapacity;
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public void setBrokerConfig(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
    }

    public class ServiceDetails {
        String name;
        String version;
        String instanceId;
        String hostname;
        String ip;
        Long pid;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }

        public String getInstanceId() {
            return instanceId;
        }

        public void setInstanceId(String instanceId) {
            this.instanceId = instanceId;
        }

        public String getHostname() {
            return hostname;
        }

        public void setHostname(String hostname) {
            this.hostname = hostname;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public Long getPid() {
            return pid;
        }

        public void setPid(Long pid) {
            this.pid = pid;
        }
    }

    public class BrokerConfig {
        Charset charset;
        String host;
        String port;
        String userName;
        String password;
        String virtualHost;
        Boolean useSSL;
        String groupId;
        Boolean durable;
        Integer heartbeatIntervalSec;
        Long networkRecoveryIntervalMs;

        public Charset getCharset() {
            return charset;
        }

        public void setCharset(Charset charset) {
            this.charset = charset;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public String getPort() {
            return port;
        }

        public void setPort(String port) {
            this.port = port;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getVirtualHost() {
            return virtualHost;
        }

        public void setVirtualHost(String virtualHost) {
            this.virtualHost = virtualHost;
        }

        public Boolean getUseSSL() {
            return useSSL;
        }

        public void setUseSSL(Boolean useSSL) {
            this.useSSL = useSSL;
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public Boolean getDurable() {
            return durable;
        }

        public void setDurable(Boolean durable) {
            this.durable = durable;
        }

        public Integer getHeartbeatIntervalSec() {
            return heartbeatIntervalSec;
        }

        public void setHeartbeatIntervalSec(Integer heartbeatIntervalSec) {
            this.heartbeatIntervalSec = heartbeatIntervalSec;
        }

        public Long getNetworkRecoveryIntervalMs() {
            return networkRecoveryIntervalMs;
        }

        public void setNetworkRecoveryIntervalMs(Long networkRecoveryIntervalMs) {
            this.networkRecoveryIntervalMs = networkRecoveryIntervalMs;
        }
    }

}
