package io.github.tcdl.msb.mock.adapterfactory;

import io.github.tcdl.msb.adapters.AdapterFactory;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.adapters.ProducerAdapter;
import io.github.tcdl.msb.api.MessageDestination;
import io.github.tcdl.msb.config.MsbConfig;
import org.apache.commons.lang3.StringUtils;

import java.util.Set;

/**
 * This AdapterFactory implementation is used to capture/submit raw messages as JSON and could be used during testing.
 */
public class TestMsbAdapterFactory implements AdapterFactory {

    private TestMsbStorageForAdapterFactory storage = new TestMsbStorageForAdapterFactory();

    public TestMsbStorageForAdapterFactory getStorage() {
        return storage;
    }

    public void setStorage(TestMsbStorageForAdapterFactory storage) {
        this.storage = storage;
    }

    @Override
    public void init(MsbConfig msbConfig) {

    }

    @Override
    public ProducerAdapter createProducerAdapter(String namespace) {
        TestMsbProducerAdapter producerAdapter = new TestMsbProducerAdapter(namespace, storage);
        storage.addProducerAdapter(namespace, producerAdapter);
        return producerAdapter;
    }

    @Override
    public ProducerAdapter createProducerAdapter(MessageDestination destination) {
        String namespace = destination.getTopic();
        TestMsbProducerAdapter producerAdapter = new TestMsbProducerAdapter(namespace, storage);
        storage.addProducerAdapter(namespace, producerAdapter);
        return producerAdapter;
    }

    @Override
    public ConsumerAdapter createConsumerAdapter(String namespace, boolean isResponseTopic) {
        TestMsbConsumerAdapter consumerAdapter = new TestMsbConsumerAdapter(namespace, storage);
        storage.addConsumerAdapter(namespace, consumerAdapter);
        return consumerAdapter;
    }

    @Override
    public ConsumerAdapter createConsumerAdapter(String namespace, Set<String> routingKeys) {
        TestMsbConsumerAdapter consumerAdapter = new TestMsbConsumerAdapter(namespace, storage);
        storage.addConsumerAdapter(namespace, routingKeys, consumerAdapter);
        return consumerAdapter;
    }

    @Override
    public boolean isUseMsbThreadingModel() {
        return false;
    }

    @Override
    public void shutdown() {

    }
}
