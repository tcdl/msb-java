package io.github.tcdl.msb.mock.adapterfactory;

import io.github.tcdl.msb.adapters.ProducerAdapter;

public class TestMsbProducerAdapter implements ProducerAdapter {

    private final String namespace;

    private final TestMsbStorageForAdapterFactory storage;

    public TestMsbProducerAdapter(String namespace, TestMsbStorageForAdapterFactory storage) {
        this.namespace = namespace;
        this.storage = storage;
    }

    @Override
    public void publish(String jsonMessage) {
        storage.addPublishedTestMessage(namespace, jsonMessage);
        storage.publishIncomingMessage(namespace, jsonMessage);
    }
}
