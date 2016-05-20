package io.github.tcdl.msb.threading;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.concurrent.*;

public class ConsumerExecutorFactoryImpl implements ConsumerExecutorFactory {

    protected static final int QUEUE_SIZE_UNLIMITED = -1;

    @Override
    public ExecutorService createConsumerThreadPool(int numberOfThreads, int queueCapacity) {
        BasicThreadFactory threadFactory = new BasicThreadFactory.Builder()
                .namingPattern("msb-consumer-thread-%d")
                .build();

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
