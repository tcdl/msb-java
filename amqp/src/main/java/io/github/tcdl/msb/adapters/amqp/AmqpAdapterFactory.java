package io.github.tcdl.msb.adapters.amqp;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Recoverable;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.tcdl.msb.adapters.AdapterFactory;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.api.*;
import io.github.tcdl.msb.api.exception.AdapterCreationException;
import io.github.tcdl.msb.api.exception.ChannelException;
import io.github.tcdl.msb.api.exception.ConfigurationException;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.config.amqp.AmqpBrokerConfig;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

/**
 * AmqpAdapterFactory is an implementation of {@link AdapterFactory}
 * for {@link AmqpProducerAdapter} and {@link AmqpConsumerAdapter}
 */
public class AmqpAdapterFactory implements AdapterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpAdapterFactory.class);

    private volatile AmqpBrokerConfig amqpBrokerConfig;
    private volatile AmqpConnectionManager connectionManager;

    /**
     * @throws ChannelException if an error is encountered during connecting to broker
     * @throws ConfigurationException if provided configuration is broken
     */
    public void init(MsbConfig msbConfig) {
        amqpBrokerConfig = createAmqpBrokerConfig(msbConfig);
        LOG.debug("MSB AMQP Broker configuration {}", amqpBrokerConfig);
        ConnectionFactory connectionFactory = createConnectionFactory(amqpBrokerConfig);
        Connection connection = createConnection(connectionFactory);
        connectionManager = createConnectionManager(connection);
    }

    //TODO extract config loading from this class and then rewrite unit test for this class completely
    protected AmqpBrokerConfig createAmqpBrokerConfig(MsbConfig msbConfig) {
        Config amqpApplicationConfig = msbConfig.getBrokerConfig();
        Config amqpLibConfig = ConfigFactory.load("amqp").getConfig("config.amqp");

        Config commonConfig = ConfigFactory.defaultOverrides()
                .withFallback(amqpApplicationConfig)
                .withFallback(amqpLibConfig);

        AmqpBrokerConfig brokerConfig = new AmqpBrokerConfig.AmqpBrokerConfigBuilder().withConfig(commonConfig).build();
        if (brokerConfig != null && !brokerConfig.getGroupId().isPresent()) {
            brokerConfig.setGroupId(Optional.of(msbConfig.getServiceDetails().getName()));
        }    
        return brokerConfig;
    }
    
    @Override
    public AmqpProducerAdapter createProducerAdapter(String topic, RequestOptions requestOptions) {
        Validate.notNull(topic, "topic is mandatory");
        Validate.notNull(requestOptions, "requestOptions are mandatory");

        Class<? extends RequestOptions> requestOptionsClass = requestOptions.getClass();
        ExchangeType exchangeType;

        if (AmqpRequestOptions.class.isAssignableFrom(requestOptionsClass)) {
            exchangeType = ((AmqpRequestOptions) requestOptions).getExchangeType();
        } else if (requestOptionsClass.equals(RequestOptions.class)) {
            exchangeType = amqpBrokerConfig.getDefaultExchangeType();
        } else {
            throw new AdapterCreationException("Illegal for this AdapterFactory RequestOptions subclass");
        }

        return new AmqpProducerAdapter(topic, exchangeType, amqpBrokerConfig, connectionManager);
    }

    @Override
    public AmqpConsumerAdapter createConsumerAdapter(String topic, boolean isResponseTopic) {
        return new AmqpConsumerAdapter(topic, amqpBrokerConfig.getDefaultExchangeType(),
                ResponderOptions.DEFAULTS.getBindingKeys(),
                amqpBrokerConfig, connectionManager, isResponseTopic);
    }

    @Override
    public AmqpConsumerAdapter createConsumerAdapter(String topic, ResponderOptions responderOptions, boolean isResponseTopic) {
        Validate.notEmpty(topic, "topic is mandatory");
        Validate.notNull(responderOptions, "responderOptions are mandatory");

        Class<? extends ResponderOptions> responderOptionsClass = responderOptions.getClass();
        ExchangeType exchangeType;

        if (AmqpResponderOptions.class.isAssignableFrom(responderOptionsClass)) {
            exchangeType = ((AmqpResponderOptions) responderOptions).getExchangeType();
        } else if (responderOptionsClass.equals(ResponderOptions.class)) {
            exchangeType = amqpBrokerConfig.getDefaultExchangeType();
        } else {
            throw new AdapterCreationException("Illegal for this AdapterFactory ResponderOptions subclass");
        }

        return new AmqpConsumerAdapter(topic, exchangeType, responderOptions.getBindingKeys(), amqpBrokerConfig, connectionManager, isResponseTopic);
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
        connectionFactory.setAutomaticRecoveryEnabled(true);
        connectionFactory.setNetworkRecoveryInterval(adapterConfig.getNetworkRecoveryIntervalMs());
        connectionFactory.setRequestedHeartbeat(adapterConfig.getHeartbeatIntervalSec());
        if (username.isPresent()) {
            connectionFactory.setUsername(username.get());
        }
        if (password.isPresent()) {
            connectionFactory.setPassword(password.get());
        }
        if (virtualHost.isPresent()) {
            connectionFactory.setVirtualHost(virtualHost.get());
        }

        try {
            if (adapterConfig.useSSL()) {
                LOG.info("Configuring adapter to use SSL...");
                connectionFactory.useSslProtocol();
                LOG.info("Configured adapter to use SSL");
            } else {
                LOG.info("Not using SSL");
            }
        } catch (Exception e) {
            LOG.error("Error while configuring SSL", e);
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
            LOG.info("Opening AMQP connection to host = {}, port = {}, username = {}, password = xxx, virtualHost = {}...",
                    connectionFactory.getHost(), connectionFactory.getPort(), connectionFactory.getUsername(), connectionFactory.getVirtualHost());
            Connection connection = connectionFactory.newConnection();
            if (connection instanceof Recoverable) {
                // This cast is possible for connections created by a factory that supports auto-recovery
                ((Recoverable) connection).addRecoveryListener(recoverable -> LOG.info("AMQP connection recovered."));
            }
            LOG.info("AMQP connection opened.");
            return connection;
        } catch (IOException | TimeoutException e) {
            throw new ChannelException("Failed to obtain connection to AMQP broker", e);
        }
    }

    @Override
    public boolean isUseMsbThreadingModel() {
        return true;
    }

    @Override
    public void shutdown() {
        try {
            connectionManager.close();
        } catch (IOException e) {
            LOG.error("Error while closing AMQP connection", e);
        }
    }

    AmqpBrokerConfig getAmqpBrokerConfig() {
        return amqpBrokerConfig;
    }

    AmqpConnectionManager getConnectionManager() {
        return connectionManager;
    }

}
