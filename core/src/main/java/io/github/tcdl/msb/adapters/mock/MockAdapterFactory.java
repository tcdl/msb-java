package io.github.tcdl.msb.adapters.mock;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;

import io.github.tcdl.msb.impl.SimpleMessageHandlerInvokeAdapterImpl;
import io.github.tcdl.msb.adapters.AdapterFactory;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.adapters.MessageHandlerInvokeAdapter;
import io.github.tcdl.msb.adapters.ProducerAdapter;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.support.Utils;

/**
 * MockAdapterFactory is an implementation of {@link AdapterFactory}
 * for {@link MockAdapter}
 */
public class MockAdapterFactory implements AdapterFactory {

    Queue<ExecutorService> consumerExecutors = new ConcurrentLinkedQueue<>();

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
        return new MockAdapter(topic, consumerExecutors);
    }

    @Override
    public MessageHandlerInvokeAdapter createMessageHandlerInvokeAdapter(String topic) {
        return new SimpleMessageHandlerInvokeAdapterImpl();
    }

    @Override
    public void shutdown() {
        for (ExecutorService executorService : consumerExecutors) {
            Utils.gracefulShutdown(executorService, "consumer");
        }

        consumerExecutors.clear();
    }

}
