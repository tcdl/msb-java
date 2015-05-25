package io.github.tcdl.adapters;

import java.lang.reflect.Constructor;

import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbConfigurations.BrokerAdapter;

/**
 * AdapterFactory creates an instance of Broker Adapter accordingly to MSB Configuration.
 * Represented as a Singleton.
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
            try {
                Class<?> clazz = Class.forName("io.github.tcdl.adapters.amqp.AmqpAdapter");
                Constructor<?> constructor = clazz.getConstructor(String.class, MsbConfigurations.class);
                Object amqpAdapter = constructor.newInstance(topic, msbConfig);
                if (!(amqpAdapter instanceof Adapter)) {
                    throw new RuntimeException("Inconsistent AMQP Adapter class");
                }
                return (Adapter) amqpAdapter;
            } catch (Exception e) {
                throw new RuntimeException("Failed to create AMQP broker", e);
            }
        } else {
            return new MockAdapter(topic, null);
        }
    }
}
