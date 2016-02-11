package io.github.tcdl.msb.testsupport.adapterfactory;

import io.github.tcdl.msb.adapters.AdapterFactory;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.adapters.MessageHandlerInvokeStrategy;
import io.github.tcdl.msb.adapters.ProducerAdapter;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.impl.SimpleMessageHandlerInvokeStrategyImpl;

/**
 * This AdapterFactory implementation is used to capture/submit raw messages as JSON and could be used during testing.
 */
public class TestMsbAdapterFactory implements AdapterFactory {

    @Override
    public void init(MsbConfig msbConfig) {

    }

    @Override
    public ProducerAdapter createProducerAdapter(String namespace) {
        TestMsbProducerAdapter producerAdapter = new TestMsbProducerAdapter(namespace);
        TestMsbStorageForAdapterFactory.Internal.addProducerAdapter(namespace, producerAdapter);
        return producerAdapter;
    }

    @Override
    public ConsumerAdapter createConsumerAdapter(String namespace, boolean isResponseTopic) {
        TestMsbConsumerAdapter consumerAdapter = new TestMsbConsumerAdapter(namespace);
        TestMsbStorageForAdapterFactory.Internal.addConsumerAdapter(namespace, consumerAdapter);
        return consumerAdapter;
    }

    @Override
    public MessageHandlerInvokeStrategy createMessageHandlerInvokeStrategy(String topic) {
        return new SimpleMessageHandlerInvokeStrategyImpl();
    }

    @Override
    public void shutdown() {

    }
}
