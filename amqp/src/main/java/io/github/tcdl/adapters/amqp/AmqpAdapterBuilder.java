package io.github.tcdl.adapters.amqp;

import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.adapters.AdapterBuilder;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.amqp.AmqpBrokerConfig;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * AmqpAdapterBuilder implement Builder pattern and creates new instance of {@link AmqpAdapter}.
 * @author ysavchuk
 *
 */
public class AmqpAdapterBuilder implements AdapterBuilder {
    private String topic;
    private AmqpBrokerConfig amqpBrokerConfig;

    @Override
    public void setTopic(String topic) {
        this.topic = topic; 
    }

    @Override
    public void setMsbConfigurations(MsbConfigurations msbConfig) {
        Config amqpApplicationConfig = msbConfig.getBrokerConfig();
        Config amqpLibConfig = ConfigFactory.load("amqp").getConfig("config.amqp");
        
        Config commonConfig = ConfigFactory.defaultOverrides()
                .withFallback(amqpApplicationConfig)
                .withFallback(amqpLibConfig);
        
        this.amqpBrokerConfig = new AmqpBrokerConfig.AmqpBrokerConfigBuilder(commonConfig).build();

        if (this.amqpBrokerConfig != null && this.amqpBrokerConfig.getGroupId() == null) {
            this.amqpBrokerConfig.setGroupId(msbConfig.getServiceDetails().getName());
        }
        
    }

    @Override
    public Adapter createAdapter() {
        return new AmqpAdapter(this.topic, this.amqpBrokerConfig);
    }

}
