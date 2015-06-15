package io.github.tcdl.adapters.amqp;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.tcdl.adapters.AdapterFactory;
import io.github.tcdl.adapters.ConsumerAdapter;
import io.github.tcdl.adapters.ProducerAdapter;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.amqp.AmqpBrokerConfig;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * AmqpAdapterFactory is an implementation of {@link AdapterFactory}
 * for {@link AmqpProducerAdapter} and {@link AmqpConsumerAdapter}
 */
public class AmqpAdapterFactory implements AdapterFactory {
    private static final Logger LOG = LoggerFactory.getLogger(AmqpAdapterFactory.class);

    private AmqpBrokerConfig amqpBrokerConfig;
    private AmqpConnectionManager connectionManager;
    private ExecutorService consumerThreadPool;

    public void init(MsbConfigurations msbConfig) {
        Config amqpApplicationConfig = msbConfig.getBrokerConfig();
        Config amqpLibConfig = ConfigFactory.load("amqp").getConfig("config.amqp");

        Config commonConfig = ConfigFactory.defaultOverrides()
                .withFallback(amqpApplicationConfig)
                .withFallback(amqpLibConfig);

        amqpBrokerConfig = new AmqpBrokerConfig.AmqpBrokerConfigBuilder(commonConfig).build();

        if (amqpBrokerConfig != null && amqpBrokerConfig.getGroupId() == null) {
            amqpBrokerConfig.setGroupId(msbConfig.getServiceDetails().getName());
        }

        connectionManager = new AmqpConnectionManager(amqpBrokerConfig);
        consumerThreadPool = createConsumerThreadPool(amqpBrokerConfig);

        Runtime.getRuntime().addShutdownHook(new Thread("AMQP adapter shutdown hook") {
            @Override
            public void run() {
                LOG.info("Invoking shutdown hook...");
                close();
                LOG.info("Shutdown hook has been invoked.");
            }
        });
    }

    @Override
    public ProducerAdapter createProducerAdapter(String topic) {
        return new AmqpProducerAdapter(topic, connectionManager);
    }

    @Override
    public ConsumerAdapter createConsumerAdapter(String topic) {
        return new AmqpConsumerAdapter(topic, amqpBrokerConfig, connectionManager, consumerThreadPool);
    }

    @Override
    public void close() {
        LOG.info("Shutting down consumer thread pool...");
        consumerThreadPool.shutdown();
        try {
            while (!consumerThreadPool.awaitTermination(10, TimeUnit.SECONDS)) {
                LOG.info("Consumer thread pool has still some work to do. Waiting...");
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting for termination", e);
        }
        LOG.info("Consumer thread pool has been shut down.");

        try {
            connectionManager.close();
        } catch (IOException e) {
            LOG.error("Error while closing AMQP connection", e);
        }
    }

    private ExecutorService createConsumerThreadPool(AmqpBrokerConfig amqpBrokerConfig) {
        BasicThreadFactory threadFactory = new BasicThreadFactory.Builder()
                .namingPattern("amqp-consumer-thread-%d")
                .build();
        return Executors.newFixedThreadPool(amqpBrokerConfig.getConsumerThreadPoolSize(), threadFactory);
    }

}
