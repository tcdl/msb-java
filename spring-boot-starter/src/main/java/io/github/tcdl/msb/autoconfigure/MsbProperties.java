package io.github.tcdl.msb.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.nio.charset.Charset;

@ConfigurationProperties("msb-config")
public class MsbProperties {

    ServiceDetails serviceDetails = new ServiceDetails();
    String brokerAdapterFactory;
    Integer timerThreadPoolSize;
    Boolean validateMessage;
    ThreadingConfig threadingConfig = new ThreadingConfig();
    BrokerConfig brokerConfig = new BrokerConfig();
    MdcLogging mdcLogging = new MdcLogging();
    RequestOptions requestOptions = new RequestOptions();

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

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

    public void setBrokerConfig(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
    }

    public ThreadingConfig getThreadingConfig() {
        return threadingConfig;
    }

    public void setThreadingConfig(ThreadingConfig threadingConfig) {
        this.threadingConfig = threadingConfig;
    }

    public MdcLogging getMdcLogging() {
        return mdcLogging;
    }

    public void setMdcLogging(MdcLogging mdcLogging) {
        this.mdcLogging = mdcLogging;
    }

    public RequestOptions getRequestOptions() {
        return requestOptions;
    }

    public void setRequestOptions(RequestOptions requestOptions) {
        this.requestOptions = requestOptions;
    }

    public class ThreadingConfig {
        Integer consumerThreadPoolSize;
        Integer consumerThreadPoolQueueCapacity;

        public void setConsumerThreadPoolSize(Integer consumerThreadPoolSize) {
            this.consumerThreadPoolSize = consumerThreadPoolSize;
        }

        public Integer getConsumerThreadPoolSize() {
            return consumerThreadPoolSize;
        }

        public Integer getConsumerThreadPoolQueueCapacity() {
            return consumerThreadPoolQueueCapacity;
        }

        public void setConsumerThreadPoolQueueCapacity(Integer consumerThreadPoolQueueCapacity) {
            this.consumerThreadPoolQueueCapacity = consumerThreadPoolQueueCapacity;
        }
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

    public class MdcLogging {
        Boolean enabled;
        String splitTagsBy;
        MessageKeys messageKeys = new MessageKeys();

        public Boolean getEnabled() {
            return enabled;
        }

        public void setEnabled(Boolean enabled) {
            this.enabled = enabled;
        }

        public String getSplitTagsBy() {
            return splitTagsBy;
        }

        public void setSplitTagsBy(String splitTagsBy) {
            this.splitTagsBy = splitTagsBy;
        }

        public MessageKeys getMessageKeys() {
            return messageKeys;
        }

        public void setMessageKeys(MessageKeys messageKeys) {
            this.messageKeys = messageKeys;
        }
    }

    public class MessageKeys {
        String messageTags;
        String correlationId;

        public String getMessageTags() {
            return messageTags;
        }

        public void setMessageTags(String messageTags) {
            this.messageTags = messageTags;
        }

        public String getCorrelationId() {
            return correlationId;
        }

        public void setCorrelationId(String correlationId) {
            this.correlationId = correlationId;
        }
    }

    public class RequestOptions {
        Integer responseTimeout;

        public Integer getResponseTimeout() {
            return responseTimeout;
        }

        public void setResponseTimeout(Integer responseTimeout) {
            this.responseTimeout = responseTimeout;
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
        String defaultExchangeType;
        Integer heartbeatIntervalSec;
        Long networkRecoveryIntervalMs;
        Integer prefetchCount;

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

        public String getDefaultExchangeType() {
            return defaultExchangeType;
        }

        public void setDefaultExchangeType(String defaultExchangeType) {
            this.defaultExchangeType = defaultExchangeType;
        }

        public Integer getPrefetchCount() {
            return prefetchCount;
        }

        public void setPrefetchCount(Integer prefetchCount) {
            this.prefetchCount = prefetchCount;
        }
    }

}
