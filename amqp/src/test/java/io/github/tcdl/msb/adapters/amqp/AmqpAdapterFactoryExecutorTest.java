package io.github.tcdl.msb.adapters.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import io.github.tcdl.msb.config.MsbConfig;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import org.junit.Test;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Recoverable;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AmqpAdapterFactoryExecutorTest {
    private static final Config CONFIG = ConfigFactory.load("reference.conf");

    private static final Config CONFIG_BOUNDED = CONFIG.withFallback(ConfigFactory.load("broker_bounded_queue.conf"));
    private static final Config CONFIG_UNBOUNDED = CONFIG.withFallback(ConfigFactory.load( "broker_unbounded_queue.conf"));

    private static class MockAdapterFactory extends AmqpAdapterFactory {
        @Override
        protected Connection createConnection(ConnectionFactory connectionFactory) {
            return mock(Connection.class, withSettings().extraInterfaces(Recoverable.class));
        }
    }

    @Test
    public void testCreateConsumerThreadPoolBoundedQueue() {
        MsbConfig msbConfigurations = new MsbConfig(CONFIG_BOUNDED);

        AmqpAdapterFactory adapterFactory = new MockAdapterFactory();
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
        MsbConfig msbConfigurations = new MsbConfig(CONFIG_UNBOUNDED);

        AmqpAdapterFactory adapterFactory = new MockAdapterFactory();
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