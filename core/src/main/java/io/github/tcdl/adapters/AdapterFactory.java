package io.github.tcdl.adapters;

import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbConfigurations.BrokerAdapter;

/**
 * Created by rdro on 4/24/2015.
 */
public class AdapterFactory {

    private static AdapterFactory instance = new AdapterFactory();

    private AdapterFactory() {
    }

    public static AdapterFactory getInstance() {
        return instance;
    }

    public Adapter createAdapter(BrokerAdapter brokerName, String topic, MsbConfigurations msbConfig) {
        if (brokerName == BrokerAdapter.AMQP) {
            return new AmqpAdapter(topic, msbConfig);
        } else {
            return new MockAdapter(topic, null);
        }
    }
}
