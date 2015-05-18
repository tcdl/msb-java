package io.github.tcdl.adapters;

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

	public Adapter createAdapter(BrokerAdapter brokerName, String topic) {
		if (brokerName == BrokerAdapter.AMQP) {
			return new AmqpAdapter(topic);
		} else {
			return MockAdapter.getInstance();
		}
	}
}
