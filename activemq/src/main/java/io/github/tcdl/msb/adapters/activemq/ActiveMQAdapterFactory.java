package io.github.tcdl.msb.adapters.activemq;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.tcdl.msb.adapters.AdapterFactory;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.api.*;
import io.github.tcdl.msb.api.exception.AdapterCreationException;
import io.github.tcdl.msb.api.exception.ChannelException;
import io.github.tcdl.msb.api.exception.ConfigurationException;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.config.activemq.ActiveMQBrokerConfig;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.jms.pool.PooledConnectionFactory;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import java.util.Optional;

/**
 * ActiveMQAdapterFactory is an implementation of {@link AdapterFactory}
 * for {@link ActiveMQAdapterFactory} and {@link ActiveMQConsumerAdapter}
 */
public class ActiveMQAdapterFactory implements AdapterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQAdapterFactory.class);

    private volatile ActiveMQBrokerConfig brokerConfig;
    private volatile ActiveMQConnectionManager connectionManager;

    /**
     * @throws ChannelException if an error is encountered during connecting to broker
     * @throws ConfigurationException if provided configuration is broken
     */
    @Override
    public void init(MsbConfig msbConfig) {
        brokerConfig = createActiveMQBrokerConfig(msbConfig);
        LOG.debug("MSB ActiveMQ Broker configuration {}", brokerConfig);
        ConnectionFactory connectionFactory = createConnectionFactory(brokerConfig);
        connectionManager = createConnectionManager(connectionFactory);
    }

    private ActiveMQBrokerConfig createActiveMQBrokerConfig(MsbConfig msbConfig) {
        Config applicationConfig = msbConfig.getBrokerConfig();
        Config brokerConfig = ConfigFactory.load("activemq").getConfig("config.activemq");

        Config commonConfig = ConfigFactory.defaultOverrides()
                .withFallback(applicationConfig)
                .withFallback(brokerConfig);

        return new ActiveMQBrokerConfig.ActiveMQBrokerConfigBuilder().withConfig(commonConfig).build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ActiveMQProducerAdapter createProducerAdapter(String topic, RequestOptions requestOptions) {
        Validate.notEmpty(topic, "topic is mandatory");
        Validate.notNull(requestOptions, "subscription type is mandatory");

        Class<? extends RequestOptions> requestOptionsClass = requestOptions.getClass();
        SubscriptionType subscriptionType;

        if (ActiveMQRequestOptions.class.isAssignableFrom(requestOptionsClass)) {
            subscriptionType = ((ActiveMQRequestOptions) requestOptions).getSubscriptionType();
        } else if (requestOptionsClass.equals(RequestOptions.class)) {
            subscriptionType = brokerConfig.getDefaultSubscriptionType();
        } else {
            throw new AdapterCreationException("Illegal for this AdapterFactory RequestOptions subclass");
        }

        return new ActiveMQProducerAdapter(topic, subscriptionType, brokerConfig, connectionManager);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerAdapter createConsumerAdapter(String topic,  boolean isResponseTopic) {
        return new ActiveMQConsumerAdapter(topic, brokerConfig.getDefaultSubscriptionType(),
                ResponderOptions.DEFAULTS.getBindingKeys(),
                brokerConfig, connectionManager, isResponseTopic);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ActiveMQConsumerAdapter createConsumerAdapter(String topic, ResponderOptions responderOptions, boolean isResponseTopic) {
        Validate.notEmpty(topic, "topic is mandatory");
        Validate.notNull(responderOptions, "responderOptions are mandatory");

        Class<? extends ResponderOptions> responderOptionsClass = responderOptions.getClass();
        SubscriptionType subscriptionType;

        if (ActiveMQResponderOptions.class.isAssignableFrom(responderOptionsClass)) {
            subscriptionType = ((ActiveMQResponderOptions) responderOptions).getSubscriptionType();
        } else if (responderOptionsClass.equals(ResponderOptions.class)) {
            subscriptionType = brokerConfig.getDefaultSubscriptionType();
        } else {
            throw new AdapterCreationException("Illegal for this AdapterFactory ResponderOptions subclass");
        }

        return new ActiveMQConsumerAdapter(topic, subscriptionType, responderOptions.getBindingKeys(),
                brokerConfig, connectionManager, isResponseTopic);
    }

    private ConnectionFactory createConnectionFactory(ActiveMQBrokerConfig brokerConfig)  {
        String uri = brokerConfig.getUri();
        Optional<String> username = brokerConfig.getUsername();
        Optional<String> password = brokerConfig.getPassword();

        ActiveMQPrefetchPolicy activeMQPrefetchPolicy = new ActiveMQPrefetchPolicy();
        activeMQPrefetchPolicy.setTopicPrefetch(brokerConfig.getPrefetchCount());
        activeMQPrefetchPolicy.setDurableTopicPrefetch(brokerConfig.getPrefetchCount());
        activeMQPrefetchPolicy.setQueuePrefetch(brokerConfig.getPrefetchCount());

        RedeliveryPolicy redeliveryPolicy = new RedeliveryPolicy();
        redeliveryPolicy.setMaximumRedeliveries(0);

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(uri);
        username.ifPresent(connectionFactory::setUserName);
        password.ifPresent(connectionFactory::setPassword);
        connectionFactory.setPrefetchPolicy(activeMQPrefetchPolicy);
        connectionFactory.setRedeliveryPolicy(redeliveryPolicy);
        connectionFactory.setMaxThreadPoolSize(brokerConfig.getPrefetchCount());

        PooledConnectionFactory pooledConnectionFactory = new PooledConnectionFactory();
        pooledConnectionFactory.setConnectionFactory(connectionFactory);
        pooledConnectionFactory.setCreateConnectionOnStartup(true);
        pooledConnectionFactory.setReconnectOnException(true);
        pooledConnectionFactory.setIdleTimeout(brokerConfig.getConnectionIdleTimeout());

        return pooledConnectionFactory;
    }

    private ActiveMQConnectionManager createConnectionManager(ConnectionFactory connectionFactory) {
        return new ActiveMQConnectionManager(connectionFactory);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isUseMsbThreadingModel() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown() {
        try {
            connectionManager.close();
        } catch (JMSException e) {
            LOG.error("Error while closing ActiveMQ connection", e);
        }
    }
}

