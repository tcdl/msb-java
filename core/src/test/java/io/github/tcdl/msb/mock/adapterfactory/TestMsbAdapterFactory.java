package io.github.tcdl.msb.mock.adapterfactory;

import io.github.tcdl.msb.adapters.AdapterFactory;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.adapters.ProducerAdapter;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.ResponderOptions;
import io.github.tcdl.msb.config.MsbConfig;

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
    public ProducerAdapter createProducerAdapter(String topic, boolean isResponseTopic, RequestOptions requestOptions) {
        TestMsbProducerAdapter producerAdapter = new TestMsbProducerAdapter(topic, storage);
        storage.addProducerAdapter(topic, producerAdapter);
        return producerAdapter;
    }

    @Override
    public ConsumerAdapter createConsumerAdapter(String namespace, boolean isResponseTopic) {
        TestMsbConsumerAdapter consumerAdapter = new TestMsbConsumerAdapter(namespace, storage);
        storage.addConsumerAdapter(namespace, consumerAdapter);
        return consumerAdapter;
    }

    @Override
    public ConsumerAdapter createConsumerAdapter(String topic, ResponderOptions responderOptions, boolean isResponseTopic) {
        ResponderOptions effectiveResponderOptions = responderOptions != null ? responderOptions: ResponderOptions.DEFAULTS;
        TestMsbConsumerAdapter consumerAdapter = new TestMsbConsumerAdapter(topic, storage);
        storage.addConsumerAdapter(topic, effectiveResponderOptions.getBindingKeys(), consumerAdapter);
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
