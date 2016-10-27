package io.github.tcdl.msb.autoconfigure;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import io.github.tcdl.msb.config.MsbConfig;
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

    @Autowired
    MsbProperties msbProperties;

    @Bean
    public MsbConfig config() {
        Config config = ConfigFactory.load("defaultMsbConfig");
        config = config.withValue("msbConfig.serviceDetails.name", ConfigValueFactory.fromAnyRef(msbProperties.serviceDetails.name));
        config = config.withValue("msbConfig.serviceDetails.instanceId", ConfigValueFactory.fromAnyRef(msbProperties.serviceDetails.instanceId));

        // Service Details
        if (StringUtils.isNoneBlank(msbProperties.serviceDetails.version))
            config = config.withValue("msbConfig.serviceDetails.version", ConfigValueFactory.fromAnyRef(msbProperties.serviceDetails.version));
        if (StringUtils.isNoneBlank(msbProperties.serviceDetails.hostname))
            config = config.withValue("msbConfig.serviceDetails.version", ConfigValueFactory.fromAnyRef(msbProperties.serviceDetails.hostname));
        if (StringUtils.isNoneBlank(msbProperties.serviceDetails.ip))
            config = config.withValue("msbConfig.serviceDetails.ip", ConfigValueFactory.fromAnyRef(msbProperties.serviceDetails.ip));
        if (msbProperties.serviceDetails.pid != null)
            config = config.withValue("msbConfig.serviceDetails.pid", ConfigValueFactory.fromAnyRef(msbProperties.serviceDetails.pid));

        // Broker Adapter Factory
        if (StringUtils.isNotBlank(msbProperties.brokerAdapterFactory))
            config = config.withValue("msbConfig.brokerAdapterFactory", ConfigValueFactory.fromAnyRef(msbProperties.brokerAdapterFactory));

        // Thread pool used for scheduling ack and response timeout tasks
        if (msbProperties.timerThreadPoolSize != null)
            config = config.withValue("msbConfig.timerThreadPoolSize", ConfigValueFactory.fromAnyRef(msbProperties.timerThreadPoolSize));

        // Threading Config for Clients
        if (msbProperties.consumerThreadPoolSize != null)
            config = config.withValue("msbConfig.threadingConfig.consumerThreadPoolSize", ConfigValueFactory.fromAnyRef(msbProperties.consumerThreadPoolSize));
        if (msbProperties.consumerThreadPoolQueueCapacity != null)
            config = config.withValue("msbConfig.threadingConfig.consumerThreadPoolQueueCapacity", ConfigValueFactory.fromAnyRef(msbProperties.consumerThreadPoolQueueCapacity));

        // Enable/disable message validation against json schema
        if (msbProperties.validateMessage != null)
            config = config.withValue("msbConfig.validateMessage", ConfigValueFactory.fromAnyRef(msbProperties.validateMessage));

        //Broker Adapter Defaults
        if (StringUtils.isNotBlank(msbProperties.brokerConfig.host))
            config = config.withValue("msbConfig.brokerConfig.host", ConfigValueFactory.fromAnyRef(msbProperties.brokerConfig.host));
        if (StringUtils.isNotBlank(msbProperties.brokerConfig.port))
            config = config.withValue("msbConfig.brokerConfig.port", ConfigValueFactory.fromAnyRef(msbProperties.brokerConfig.port));
        if (StringUtils.isNotBlank(msbProperties.brokerConfig.userName))
            config = config.withValue("msbConfig.brokerConfig.username", ConfigValueFactory.fromAnyRef(msbProperties.brokerConfig.userName));
        if (StringUtils.isNotBlank(msbProperties.brokerConfig.virtualHost))
            config = config.withValue("msbConfig.brokerConfig.virtualHost", ConfigValueFactory.fromAnyRef(msbProperties.brokerConfig.virtualHost));
        if (msbProperties.brokerConfig.useSSL != null)
            config = config.withValue("msbConfig.brokerConfig.useSSL", ConfigValueFactory.fromAnyRef(msbProperties.brokerConfig.useSSL));
        if (msbProperties.brokerConfig.durable != null)
            config = config.withValue("msbConfig.brokerConfig.durable", ConfigValueFactory.fromAnyRef(msbProperties.brokerConfig.durable));
        if (msbProperties.brokerConfig.charset != null)
            config = config.withValue("msbConfig.brokerConfig.charset", ConfigValueFactory.fromAnyRef(msbProperties.brokerConfig.charset));
        if (StringUtils.isNotBlank(msbProperties.brokerConfig.groupId))
            config = config.withValue("msbConfig.brokerConfig.groupId", ConfigValueFactory.fromAnyRef(msbProperties.brokerConfig.groupId));
        if (msbProperties.brokerConfig.heartbeatIntervalSec != null)
            config = config.withValue("msbConfig.brokerConfig.heartbeatIntervalSec", ConfigValueFactory.fromAnyRef(msbProperties.brokerConfig.heartbeatIntervalSec));
        if (msbProperties.brokerConfig.networkRecoveryIntervalMs != null)
            config = config.withValue("msbConfig.brokerConfig.networkRecoveryIntervalMs", ConfigValueFactory.fromAnyRef(msbProperties.brokerConfig.networkRecoveryIntervalMs));

        return new MsbConfig(config);
    }
}
