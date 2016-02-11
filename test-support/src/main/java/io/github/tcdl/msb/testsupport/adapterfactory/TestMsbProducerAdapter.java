package io.github.tcdl.msb.testsupport.adapterfactory;

import io.github.tcdl.msb.adapters.ProducerAdapter;

public class TestMsbProducerAdapter implements ProducerAdapter {

    private final String namespace;

    public TestMsbProducerAdapter(String namespace) {
        this.namespace = namespace;
    }

    @Override
    public void publish(String jsonMessage) {
        TestMsbStorageForAdapterFactory.Internal.addPublishedTestMessage(namespace, jsonMessage);
    }
}
