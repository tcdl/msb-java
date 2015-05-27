package io.github.tcdl.adapters.mock;

import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.adapters.AdapterFactory;
import io.github.tcdl.config.MsbConfigurations;

/**
 * MockAdapterFactory is an implementation of {@link AdapterFactory}
 * for {@link MockAdapter}
 *
 */
public class MockAdapterFactory implements AdapterFactory {

    @Override
    public void init(MsbConfigurations msbConfig) {
        // No-op
    }

    @Override
    public Adapter createAdapter(String topic) {
        return new MockAdapter(topic, null);
    }

}
