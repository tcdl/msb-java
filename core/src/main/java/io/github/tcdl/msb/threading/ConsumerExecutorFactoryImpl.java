package io.github.tcdl.msb.threading;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConsumerExecutorFactoryImpl implements ConsumerExecutorFactory {

    protected static final int QUEUE_SIZE_UNLIMITED = -1;
    private final BasicThreadFactory threadFactory = new BasicThreadFactory.Builder()
            .namingPattern("msb-consumer-thread-%d")
            .build();

    @Override
    public ExecutorService createConsumerThreadPool(int numberOfThreads, int queueCapacity) {
        BlockingQueue<Runnable> queue;
        if (queueCapacity == QUEUE_SIZE_UNLIMITED) {
            queue = new LinkedBlockingQueue<>();
        } else {
            queue = new ArrayBlockingQueue<>(queueCapacity);
        }

        return new ThreadPoolExecutor(numberOfThreads, numberOfThreads,
                0L, TimeUnit.MILLISECONDS,
                queue,
                threadFactory);
    }
}
