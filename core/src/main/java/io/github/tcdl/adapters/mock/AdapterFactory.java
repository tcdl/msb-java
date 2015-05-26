package io.github.tcdl.adapters.mock;

import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.adapters.MsbAdapterFactory;
import io.github.tcdl.config.MsbConfigurations;

/**
 * Implementation of {@link MsbAdapterFactory} for MockAdapter
 * @author ysavchuk
 *
 */
public class AdapterFactory extends MsbAdapterFactory {

    
    public AdapterFactory(MsbConfigurations msbConfig) {
        super(msbConfig);
    }

    @Override
    public Adapter createAdapter(String topic) {
        return new MockAdapter(topic, null);
    }

}
