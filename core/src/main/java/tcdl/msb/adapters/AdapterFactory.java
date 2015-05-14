package tcdl.msb.adapters;

import java.util.Map;

import tcdl.msb.config.MsbConfigurations.BrokerAdapter;

import java.util.Properties;

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

	public Adapter createAdapter(BrokerAdapter brokerName, Map<String, String> brokerConf) {
		// hardcoded
		return MockAdapter.getInstance();
	}

    public Adapter createAdapter(Properties options) {
        return MockAdapter.getInstance();
        //return new AMQPAdapter();
    }

}
