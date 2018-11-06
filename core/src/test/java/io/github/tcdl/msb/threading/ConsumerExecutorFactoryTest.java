package io.github.tcdl.msb.threading;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerExecutorFactoryTest {

    private ConsumerExecutorFactoryImpl factory;

    @Before
    public void setUp() {
        factory = new ConsumerExecutorFactoryImpl();
    }

    @Test
    public void testCreateConsumerThreadPoolBoundedQueue() {
        ExecutorService consumerThreadPool = factory.createConsumerThreadPool(5, 20);

        assertNotNull(consumerThreadPool);
        assertTrue(consumerThreadPool instanceof ThreadPoolExecutor);
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) consumerThreadPool;
        assertEquals(5, threadPoolExecutor.getCorePoolSize());
        BlockingQueue<Runnable> queue = threadPoolExecutor.getQueue();
        assertNotNull(queue);
        assertTrue(queue instanceof ArrayBlockingQueue);
        assertEquals(20, queue.remainingCapacity());
    }

    @Test
    public void testCreateConsumerThreadPoolUnboundedQueue() {
          ExecutorService consumerThreadPool = factory.createConsumerThreadPool(5, -1);

        assertNotNull(consumerThreadPool);
        assertTrue(consumerThreadPool instanceof ThreadPoolExecutor);
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) consumerThreadPool;
        assertEquals(5, threadPoolExecutor.getCorePoolSize());

        BlockingQueue<Runnable> queue = threadPoolExecutor.getQueue();
        assertNotNull(queue);
        assertTrue(queue instanceof LinkedBlockingQueue);
    }
}
