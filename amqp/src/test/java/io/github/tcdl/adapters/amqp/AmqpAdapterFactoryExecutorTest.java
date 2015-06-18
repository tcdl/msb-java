package io.github.tcdl.adapters.amqp;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.tcdl.config.MsbConfigurations;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AmqpAdapterFactoryExecutorTest {

    @Test
    public void testCreateConsumerThreadPoolBoundedQueue() {
        String configStr =
                "msbConfig {"
                + "  brokerConfig = { "
                + "    consumerThreadPoolSize = 5\n"
                + "    consumerThreadPoolQueueCapacity = 20\n"
                + "  }"
                + "}";
        Config msbConfig = ConfigFactory.parseString(configStr);
        MsbConfigurations msbConfigurations = new MsbConfigurations(msbConfig);

        AmqpAdapterFactory adapterFactory = new AmqpAdapterFactory();
        adapterFactory.init(msbConfigurations);
        ExecutorService consumerThreadPool = adapterFactory.createConsumerThreadPool(adapterFactory.getAmqpBrokerConfig());

        assertNotNull(consumerThreadPool);
        assertTrue(consumerThreadPool instanceof ThreadPoolExecutor);
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) consumerThreadPool;

        BlockingQueue<Runnable> queue = threadPoolExecutor.getQueue();
        assertNotNull(queue);
        assertTrue(queue instanceof ArrayBlockingQueue);
        assertEquals(20, ((ArrayBlockingQueue) queue).remainingCapacity());
    }

    @Test
    public void testCreateConsumerThreadPoolUnboundedQueue() {
        String configStr = "msbConfig {"
                + "  brokerConfig = {"
                + "    consumerThreadPoolSize = 5\n"
                + "    consumerThreadPoolQueueCapacity = -1\n"
                + "  }"
                + "}";
        Config msbConfig = ConfigFactory.parseString(configStr);
        MsbConfigurations msbConfigurations = new MsbConfigurations(msbConfig);

        AmqpAdapterFactory adapterFactory = new AmqpAdapterFactory();
        adapterFactory.init(msbConfigurations);
        ExecutorService consumerThreadPool = adapterFactory.createConsumerThreadPool(adapterFactory.getAmqpBrokerConfig());

        assertNotNull(consumerThreadPool);
        assertTrue(consumerThreadPool instanceof ThreadPoolExecutor);
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) consumerThreadPool;

        BlockingQueue<Runnable> queue = threadPoolExecutor.getQueue();
        assertNotNull(queue);
        assertTrue(queue instanceof LinkedBlockingQueue);
    }
}