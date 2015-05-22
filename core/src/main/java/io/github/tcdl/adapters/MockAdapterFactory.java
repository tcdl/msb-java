package io.github.tcdl.adapters;

import com.typesafe.config.Config;

public class MockAdapterFactory implements AdapterFactory {

    public MockAdapterFactory(Config config) {
        //No-op
    }

    @Override
    public Adapter createAdapter(String topic) {
        return new MockAdapter(topic);
    }
}
