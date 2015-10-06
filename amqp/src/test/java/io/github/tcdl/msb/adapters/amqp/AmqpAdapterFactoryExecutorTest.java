package io.github.tcdl.msb.adapters.amqp;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Recoverable;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.tcdl.msb.config.MsbConfig;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

public class AmqpAdapterFactoryExecutorTest {

    String basicConfig = "msbConfig {"
            + "  timerThreadPoolSize = 1\n"
            + "  validateMessage = true\n"
            + "  brokerAdapterFactory = \"AmqpAdapterFactory\" \n"
            + "  serviceDetails = {"
            + "     name = \"test_msb\" \n"
            + "     version = \"1.0.1\" \n"
            + "     instanceId = \"msbd06a-ed59-4a39-9f95-811c5fb6ab87\" \n"
            + "  } \n"
            + "  %s"
            + "}";

    private static class MockAdapterFactory extends AmqpAdapterFactory {
        @Override
        protected Connection createConnection(ConnectionFactory connectionFactory) {
            return mock(Connection.class, withSettings().extraInterfaces(Recoverable.class));
        }
    }

    @Test
    public void testCreateConsumerThreadPoolBoundedQueue() {
        String brokerConf =
                " brokerConfig = { "
                        + "    charsetName = \"UTF-8\"\n"
                        + "    consumerThreadPoolSize = 5\n"
                        + "    consumerThreadPoolQueueCapacity = 20\n"
                        + "    requeueRejectedMessages = true\n"
                        + "  }";

        Config msbConfig = ConfigFactory.parseString(String.format(basicConfig, brokerConf));
        MsbConfig msbConfigurations = new MsbConfig(msbConfig);

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
        String brokerConf =
                " brokerConfig = { "
                        + "    charsetName = \"UTF-8\"\n"
                        + "    consumerThreadPoolSize = 5\n"
                        + "    consumerThreadPoolQueueCapacity = -1\n"
                        + "    requeueRejectedMessages = true\n"
                        + "  }";
        Config msbConfig = ConfigFactory.parseString(String.format(basicConfig, brokerConf));
        MsbConfig msbConfigurations = new MsbConfig(msbConfig);

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