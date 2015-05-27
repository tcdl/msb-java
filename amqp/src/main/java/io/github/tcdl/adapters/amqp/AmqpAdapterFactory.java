package io.github.tcdl.adapters.amqp;

import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.adapters.AdapterFactory;
import io.github.tcdl.adapters.mock.MockAdapter;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.amqp.AmqpBrokerConfig;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * AmqpAdapterFactory is an implementation of {@link AdapterFactory}
 * for {@link AmqpAdapter}
 *
 */
public class AmqpAdapterFactory implements AdapterFactory {
    private AmqpBrokerConfig amqpBrokerConfig;
    private AmqpConnectionManager connectionManager;

    public void init(MsbConfigurations msbConfig) {
        Config amqpApplicationConfig = msbConfig.getBrokerConfig();
        Config amqpLibConfig = ConfigFactory.load("amqp").getConfig("config.amqp");

        Config commonConfig = ConfigFactory.defaultOverrides()
                .withFallback(amqpApplicationConfig)
                .withFallback(amqpLibConfig);

        this.amqpBrokerConfig = new AmqpBrokerConfig.AmqpBrokerConfigBuilder(commonConfig).build();

        if (this.amqpBrokerConfig != null && this.amqpBrokerConfig.getGroupId() == null) {
            this.amqpBrokerConfig.setGroupId(msbConfig.getServiceDetails().getName());
        }

        this.connectionManager = new AmqpConnectionManager(this.amqpBrokerConfig);
    }

    @Override
    public Adapter createAdapter(String topic) {
        return new AmqpAdapter(topic, this.amqpBrokerConfig, this.connectionManager);
    }

}
