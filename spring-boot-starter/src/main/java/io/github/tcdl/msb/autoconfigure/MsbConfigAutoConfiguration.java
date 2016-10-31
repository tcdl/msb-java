package io.github.tcdl.msb.autoconfigure;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.support.Utils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@AutoConfigureBefore(MsbContextAutoConfiguration.class)
@EnableConfigurationProperties(MsbProperties.class)
public class MsbConfigAutoConfiguration {

    public static final String DEFAULT_VERSION = "1.0.0";
    public static final String DEFAULT_APP_NAME = Utils.generateId();
    public static final String DEFAULT_ADAPTER_FACTORY = "io.github.tcdl.msb.adapters.amqp.AmqpAdapterFactory";
    private static final boolean DEFAULT_BROKER_DURABLE = true;
    public static final int DEFAULT_TIMER_THREAD_POOL_SIZE = 2;

    @Autowired
    MsbProperties msbProperties;

    @Bean
    public MsbConfig config() {
        Config config = ConfigFactory.load("reference");
        String appName = StringUtils.isNotBlank(msbProperties.serviceDetails.name) ? msbProperties.serviceDetails.name : DEFAULT_APP_NAME;
        config = config.withValue("msbConfig.serviceDetails.name", ConfigValueFactory.fromAnyRef(appName));
        config = config.withValue("msbConfig.serviceDetails.instanceId", ConfigValueFactory.fromAnyRef(msbProperties.serviceDetails.instanceId));

        // Service Details
        String version = StringUtils.isNotBlank(msbProperties.serviceDetails.version) ? msbProperties.serviceDetails.version : DEFAULT_VERSION;
        config = config.withValue("msbConfig.serviceDetails.version", ConfigValueFactory.fromAnyRef(version));
        if (StringUtils.isNoneBlank(msbProperties.serviceDetails.hostname))
            config = config.withValue("msbConfig.serviceDetails.hostname", ConfigValueFactory.fromAnyRef(msbProperties.serviceDetails.hostname));
        if (StringUtils.isNoneBlank(msbProperties.serviceDetails.ip))
            config = config.withValue("msbConfig.serviceDetails.ip", ConfigValueFactory.fromAnyRef(msbProperties.serviceDetails.ip));
        if (msbProperties.serviceDetails.pid != null)
            config = config.withValue("msbConfig.serviceDetails.pid", ConfigValueFactory.fromAnyRef(msbProperties.serviceDetails.pid));

        // Broker Adapter Factory
        String brokerAdapterFactory = StringUtils.isNotBlank(msbProperties.brokerAdapterFactory) ? msbProperties.brokerAdapterFactory : DEFAULT_ADAPTER_FACTORY;
        config = config.withValue("msbConfig.brokerAdapterFactory", ConfigValueFactory.fromAnyRef(brokerAdapterFactory));

        // Thread pool used for scheduling ack and response timeout tasks
        Integer timerThreadPoolSize = msbProperties.timerThreadPoolSize != null ? msbProperties.timerThreadPoolSize : DEFAULT_TIMER_THREAD_POOL_SIZE;
        config = config.withValue("msbConfig.timerThreadPoolSize", ConfigValueFactory.fromAnyRef(timerThreadPoolSize));

        // Threading Config for Clients
        if (msbProperties.threadingConfig.consumerThreadPoolSize != null)
            config = config.withValue("msbConfig.threadingConfig.consumerThreadPoolSize", ConfigValueFactory.fromAnyRef(msbProperties.threadingConfig.consumerThreadPoolSize));
        if (msbProperties.threadingConfig.consumerThreadPoolQueueCapacity != null)
            config = config.withValue("msbConfig.threadingConfig.consumerThreadPoolQueueCapacity", ConfigValueFactory.fromAnyRef(msbProperties.threadingConfig.consumerThreadPoolQueueCapacity));

        // Enable/disable message validation against json schema
        if (msbProperties.validateMessage != null)
            config = config.withValue("msbConfig.validateMessage", ConfigValueFactory.fromAnyRef(msbProperties.validateMessage));

        //MDC logging
        if (msbProperties.mdcLogging.enabled != null)
            config = config.withValue("msbConfig.mdcLogging.enabled", ConfigValueFactory.fromAnyRef(msbProperties.mdcLogging.enabled));
        if (StringUtils.isNotBlank(msbProperties.mdcLogging.splitTagsBy))
            config = config.withValue("msbConfig.mdcLogging.splitTagsBy", ConfigValueFactory.fromAnyRef(msbProperties.mdcLogging.splitTagsBy));
        if (StringUtils.isNotBlank(msbProperties.mdcLogging.messageKeys.messageTags))
            config = config.withValue("msbConfig.mdcLogging.messageKeys.messageTags", ConfigValueFactory.fromAnyRef(msbProperties.mdcLogging.messageKeys.messageTags));
        if (StringUtils.isNotBlank(msbProperties.mdcLogging.messageKeys.correlationId))
            config = config.withValue("msbConfig.mdcLogging.messageKeys.correlationId", ConfigValueFactory.fromAnyRef(msbProperties.mdcLogging.messageKeys.correlationId));

        //requestOptions
        if (msbProperties.requestOptions.responseTimeout != null)
            config = config.withValue("msbConfig.requestOptions.responseTimeout", ConfigValueFactory.fromAnyRef(msbProperties.requestOptions.responseTimeout));

        //Broker Adapter Defaults
        if (StringUtils.isNotBlank(msbProperties.brokerConfig.host))
            config = config.withValue("msbConfig.brokerConfig.host", ConfigValueFactory.fromAnyRef(msbProperties.brokerConfig.host));
        if (StringUtils.isNotBlank(msbProperties.brokerConfig.port))
            config = config.withValue("msbConfig.brokerConfig.port", ConfigValueFactory.fromAnyRef(msbProperties.brokerConfig.port));
        if (StringUtils.isNotBlank(msbProperties.brokerConfig.userName))
            config = config.withValue("msbConfig.brokerConfig.username", ConfigValueFactory.fromAnyRef(msbProperties.brokerConfig.userName));
        if (StringUtils.isNotBlank(msbProperties.brokerConfig.password))
            config = config.withValue("msbConfig.brokerConfig.password", ConfigValueFactory.fromAnyRef(msbProperties.brokerConfig.password));
        if (StringUtils.isNotBlank(msbProperties.brokerConfig.virtualHost))
            config = config.withValue("msbConfig.brokerConfig.virtualHost", ConfigValueFactory.fromAnyRef(msbProperties.brokerConfig.virtualHost));
        if (msbProperties.brokerConfig.useSSL != null)
            config = config.withValue("msbConfig.brokerConfig.useSSL", ConfigValueFactory.fromAnyRef(msbProperties.brokerConfig.useSSL));
        boolean durable = msbProperties.brokerConfig.durable != null ? msbProperties.brokerConfig.durable : DEFAULT_BROKER_DURABLE;
        config = config.withValue("msbConfig.brokerConfig.durable", ConfigValueFactory.fromAnyRef(durable));
        if (msbProperties.brokerConfig.charset != null)
            config = config.withValue("msbConfig.brokerConfig.charset", ConfigValueFactory.fromAnyRef(msbProperties.brokerConfig.charset));
        if (StringUtils.isNotBlank(msbProperties.brokerConfig.groupId))
            config = config.withValue("msbConfig.brokerConfig.groupId", ConfigValueFactory.fromAnyRef(msbProperties.brokerConfig.groupId));
        if (msbProperties.brokerConfig.heartbeatIntervalSec != null)
            config = config.withValue("msbConfig.brokerConfig.heartbeatIntervalSec", ConfigValueFactory.fromAnyRef(msbProperties.brokerConfig.heartbeatIntervalSec));
        if (msbProperties.brokerConfig.networkRecoveryIntervalMs != null)
            config = config.withValue("msbConfig.brokerConfig.networkRecoveryIntervalMs", ConfigValueFactory.fromAnyRef(msbProperties.brokerConfig.networkRecoveryIntervalMs));
        if (StringUtils.isNotBlank(msbProperties.brokerConfig.defaultExchangeType))
            config = config.withValue("msbConfig.brokerConfig.defaultExchangeType", ConfigValueFactory.fromAnyRef(msbProperties.brokerConfig.defaultExchangeType));
        if (msbProperties.brokerConfig.prefetchCount != null)
            config = config.withValue("msbConfig.brokerConfig.prefetchCount", ConfigValueFactory.fromAnyRef(msbProperties.brokerConfig.prefetchCount));

        return new MsbConfig(config);
    }
}
