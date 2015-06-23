package io.github.tcdl.adapters.amqp;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.github.tcdl.adapters.AdapterFactory;
import io.github.tcdl.adapters.ConsumerAdapter;
import io.github.tcdl.adapters.ProducerAdapter;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.amqp.AmqpBrokerConfig;
import io.github.tcdl.exception.ChannelException;
import io.github.tcdl.exception.ConfigurationException;

import io.github.tcdl.support.Utils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * AmqpAdapterFactory is an implementation of {@link AdapterFactory}
 * for {@link AmqpProducerAdapter} and {@link AmqpConsumerAdapter}
 */
public class AmqpAdapterFactory implements AdapterFactory {
    private static final Logger LOG = LoggerFactory.getLogger(AmqpAdapterFactory.class);

    private static final int QUEUE_SIZE_UNLIMITED = -1;

    private AmqpBrokerConfig amqpBrokerConfig;
    private AmqpConnectionManager connectionManager;
    private ExecutorService consumerThreadPool;

    /**
     * @throws ChannelException if an error is encountered during connecting to broker
     * @throws ConfigurationException if provided configuration is broken
     */
    public void init(MsbConfigurations msbConfig) {
        amqpBrokerConfig = createAmqpBrokerConfig(msbConfig);
        ConnectionFactory connectionFactory = createConnectionFactory(amqpBrokerConfig);
        Connection connection = createConnection(connectionFactory);
        connectionManager = createConnectionManager(connection);        
        consumerThreadPool = createConsumerThreadPool(amqpBrokerConfig);
    }

    protected AmqpBrokerConfig createAmqpBrokerConfig(MsbConfigurations msbConfig) {
        Config amqpApplicationConfig = msbConfig.getBrokerConfig();
        Config amqpLibConfig = ConfigFactory.load("amqp").getConfig("config.amqp");

        Config commonConfig = ConfigFactory.defaultOverrides()
                .withFallback(amqpApplicationConfig)
                .withFallback(amqpLibConfig);

        AmqpBrokerConfig brokerConfig = new AmqpBrokerConfig.AmqpBrokerConfigBuilder(commonConfig).build();
        if (brokerConfig != null && brokerConfig.getGroupId() == null) {
            brokerConfig.setGroupId(msbConfig.getServiceDetails().getName());
        }    
        return brokerConfig;
    }
    
    @Override
    public ProducerAdapter createProducerAdapter(String topic) {
        return new AmqpProducerAdapter(topic, connectionManager);
    }

    @Override
    public ConsumerAdapter createConsumerAdapter(String topic) {
        return new AmqpConsumerAdapter(topic, amqpBrokerConfig, connectionManager, consumerThreadPool);
    }

    protected ConnectionFactory createConnectionFactory(AmqpBrokerConfig adapterConfig) {
        String host = adapterConfig.getHost();
        int port = adapterConfig.getPort();
        Optional<String> username = adapterConfig.getUsername();
        Optional<String> password = adapterConfig.getPassword();
        Optional<String> virtualHost = adapterConfig.getVirtualHost();

        ConnectionFactory connectionFactory = createConnectionFactory();
        connectionFactory.setHost(host);
        connectionFactory.setPort(port);
        if(username.isPresent()) {
            connectionFactory.setUsername(username.get());
        }
        if(password.isPresent()) {
            connectionFactory.setPassword(password.get());
        }
        if(virtualHost.isPresent()) {
            connectionFactory.setVirtualHost(virtualHost.get());
        }
        return connectionFactory;
    }
    
    protected ConnectionFactory createConnectionFactory() {
        return new ConnectionFactory();
    }
    
    protected AmqpConnectionManager createConnectionManager(Connection connection) {
        return new AmqpConnectionManager(connection);
    }

    /**
     * @throws ChannelException if some problems during connecting to Broker were occurred
     */
    protected Connection createConnection(ConnectionFactory connectionFactory) {
        try {
            LOG.info(String.format("Opening AMQP connection to host = %s, port = %s, username = %s, password = xxx, virtualHost = %s...",
                    connectionFactory.getHost(), connectionFactory.getPort(), connectionFactory.getUsername(), connectionFactory.getVirtualHost()));
            Connection connection = connectionFactory.newConnection();
            LOG.info("AMQP connection opened.");
            return connection;
        } catch (IOException e) {
            throw new ChannelException("Failed to obtain connection to AMQP broker", e);
        }
    }

    @Override
    public void shutdown() {
        Utils.gracefulShutdown(consumerThreadPool, "consumer");

        try {
            connectionManager.close();
        } catch (IOException e) {
            LOG.error("Error while closing AMQP connection", e);
        }
    }

    protected ExecutorService createConsumerThreadPool(AmqpBrokerConfig amqpBrokerConfig) {
        BasicThreadFactory threadFactory = new BasicThreadFactory.Builder()
                .namingPattern("amqp-consumer-thread-%d")
                .build();
        int numberOfThreads = amqpBrokerConfig.getConsumerThreadPoolSize();
        int queueCapacity = amqpBrokerConfig.getConsumerThreadPoolQueueCapacity();

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

    AmqpBrokerConfig getAmqpBrokerConfig() {
        return amqpBrokerConfig;
    }

    AmqpConnectionManager getConnectionManager() {
        return connectionManager;
    }

    ExecutorService getConsumerThreadPool() {
        return consumerThreadPool;
    }
    
    

}
