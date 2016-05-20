package io.github.tcdl.msb.threading;

import java.util.concurrent.ExecutorService;

/**
 * Implementations define a way to create {@link ExecutorService} instances
 * used to invoke {@link io.github.tcdl.msb.MessageHandler}.
 */
public interface ConsumerExecutorFactory {
    ExecutorService createConsumerThreadPool(int numberOfThreads, int queueCapacity);
}
