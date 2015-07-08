package io.github.tcdl.adapters.mock;

import io.github.tcdl.adapters.AdapterFactory;
import io.github.tcdl.adapters.ConsumerAdapter;
import io.github.tcdl.adapters.ProducerAdapter;
import io.github.tcdl.config.MsbConfig;

/**
 * MockAdapterFactory is an implementation of {@link AdapterFactory}
 * for {@link MockAdapter}
 *
 */
public class MockAdapterFactory implements AdapterFactory {

    @Override
    public void init(MsbConfig msbConfig) {
        // No-op
    }

    @Override
    public ProducerAdapter createProducerAdapter(String topic) {
        return new MockAdapter(topic);
    }

    @Override
    public ConsumerAdapter createConsumerAdapter(String topic) {
        return new MockAdapter(topic);
    }

    @Override
    public void shutdown() {
        // No-op
    }
}
