package io.github.tcdl.msb.adapters.activemq;

import io.github.tcdl.msb.adapters.ProducerAdapter;
import io.github.tcdl.msb.api.SubscriptionType;
import io.github.tcdl.msb.api.exception.ChannelException;
import io.github.tcdl.msb.config.activemq.ActiveMQBrokerConfig;
import joptsimple.internal.Strings;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Message;
import javax.jms.MessageProducer;

public class ActiveMQProducerAdapter implements ProducerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQProducerAdapter.class);
    private static final String ERROR_MESSAGE_TEMPLATE = "Failed to publish message to topic '%s' with routing key '%s'";

    private String topic;
    private ActiveMQRecoverableSession session;
    private MessageProducer producer;

    ActiveMQProducerAdapter(String topic, SubscriptionType subscriptionType, ActiveMQBrokerConfig brokerConfig, ActiveMQConnectionManager connectionManager) {
        Validate.notNull(topic, "Topic is mandatory");
        Validate.notNull(subscriptionType, "Subscription type is mandatory");
        Validate.notNull(brokerConfig, "Broker config is mandatory");
        Validate.notNull(connectionManager, "Connection manager is mandatory");

        this.topic = topic;

        try {
            this.session = ActiveMQRecoverableSession.instance(connectionManager);
            producer = session.createProducer(topic, subscriptionType, brokerConfig.isDurable());
        } catch (Exception e) {
            throw new ChannelException("Failed to setup channel from ActiveMQ connection", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(String jsonMessage) {
        publish(jsonMessage, Strings.EMPTY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(String jsonMessage, String routingKey) {
        try {
            Message message = session.createMessage(jsonMessage, routingKey);
            LOG.debug("Publishing message. Topic name = [{}], routing key = [{}]", topic, routingKey);
            producer.send(message);
        } catch (Exception e) {
            LOG.error(ERROR_MESSAGE_TEMPLATE, topic, routingKey);
            LOG.trace("Message: {}", jsonMessage);
            throw new ChannelException(String.format(ERROR_MESSAGE_TEMPLATE, topic, routingKey), e);
        }
    }
}
