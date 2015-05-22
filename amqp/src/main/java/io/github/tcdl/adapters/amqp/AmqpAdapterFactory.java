package io.github.tcdl.adapters.amqp;


import com.typesafe.config.Config;
import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.adapters.AdapterFactory;
import io.github.tcdl.config.MsbConfigurations;

public class AmqpAdapterFactory implements AdapterFactory {
    private final AmqpBrokerConfig adapaterConfig;
    private final AmqpConnectionManager connectionManager;

    public AmqpAdapterFactory(Config config) {
        adapaterConfig = new AmqpBrokerConfig.AmqpBrokerConfigBuilder(config).build();
        connectionManager = new AmqpConnectionManager(adapaterConfig);
    }

    @Override
    public Adapter createAdapter(String topic) {
        return new AmqpAdapter(topic, adapaterConfig, connectionManager);
    }
}
