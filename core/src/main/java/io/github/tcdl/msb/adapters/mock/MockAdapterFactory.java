package io.github.tcdl.msb.adapters.mock;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import io.github.tcdl.msb.impl.SimpleMessageHandlerInvokeStrategyImpl;
import io.github.tcdl.msb.adapters.AdapterFactory;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.adapters.MessageHandlerInvokeStrategy;
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
    public MessageHandlerInvokeStrategy createMessageHandlerInvokeStrategy(String topic) {
        return new SimpleMessageHandlerInvokeStrategyImpl();
    }

    @Override
    public void shutdown() {
        for (ExecutorService executorService : consumerExecutors) {
            Utils.gracefulShutdown(executorService, "consumer");
        }

        consumerExecutors.clear();
    }

}
