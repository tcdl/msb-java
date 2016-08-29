package io.github.tcdl.msb.mock.adapterfactory;

import io.github.tcdl.msb.adapters.ProducerAdapter;
import org.apache.commons.lang3.StringUtils;

import java.util.Set;

public class TestMsbProducerAdapter implements ProducerAdapter {

    private final String namespace;

    private final TestMsbStorageForAdapterFactory storage;

    public TestMsbProducerAdapter(String namespace, TestMsbStorageForAdapterFactory storage) {
        this.namespace = namespace;
        this.storage = storage;
    }

    @Override
    public void publish(String jsonMessage) {
        storage.addPublishedTestMessage(namespace, StringUtils.EMPTY, jsonMessage);
        storage.publishIncomingMessage(namespace, StringUtils.EMPTY, jsonMessage);
    }

    @Override
    public void publish(String jsonMessage, String routingKey) {
        storage.addPublishedTestMessage(namespace, routingKey, jsonMessage);
        storage.publishIncomingMessage(namespace, routingKey, jsonMessage);
    }
}
