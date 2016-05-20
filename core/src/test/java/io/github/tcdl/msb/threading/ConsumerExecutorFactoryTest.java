package io.github.tcdl.msb.threading;

import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.config.MsbConfig;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import io.github.tcdl.msb.threading.ExecutorBasedMessageHandlerInvoker;
import io.github.tcdl.msb.threading.MessageProcessingTask;
import io.github.tcdl.msb.threading.ThreadPoolMessageHandlerInvoker;
import org.junit.Before;
import org.junit.Test;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerExecutorFactoryTest {

    ConsumerExecutorFactoryImpl factory;

    @Before
    public void setUp() throws Exception {
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
        assertEquals(20, ((ArrayBlockingQueue) queue).remainingCapacity());
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
