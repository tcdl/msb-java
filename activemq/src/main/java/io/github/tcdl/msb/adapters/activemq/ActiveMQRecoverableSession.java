package io.github.tcdl.msb.adapters.activemq;

import io.github.tcdl.msb.api.SubscriptionType;
import io.github.tcdl.msb.api.exception.ChannelException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.Set;
import java.util.stream.Collectors;

import static io.github.tcdl.msb.api.SubscriptionType.QUEUE;

public class ActiveMQRecoverableSession {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQRecoverableSession.class);

    private static ActiveMQRecoverableSession instance;
    private ActiveMQConnectionManager connectionManager;

    private ActiveMQRecoverableSession(ActiveMQConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    static ActiveMQRecoverableSession instance(ActiveMQConnectionManager connectionManager) {
        if (instance == null) {
            instance = new ActiveMQRecoverableSession(connectionManager);
        }
        return instance;
    }

    public MessageProducer createProducer(String topic, SubscriptionType subscriptionType, boolean durable) {
        Validate.notEmpty(topic, "topic is mandatory");
        Validate.notNull(subscriptionType, "subscription type is mandatory");

        try {
            // omit destination to specify it later during sending a message
            MessageProducer producer = getSession(null).createProducer(null);
            if (durable) {
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            }
            LOG.debug("Created producer on topic '{}'", topic);

            return producer;
        } catch (JMSException e) {
            throw new ChannelException("Producer creation failed with exception", e);
        }
    }

    public MessageConsumer createConsumer(String topic, SubscriptionType subscriptionType, String clientId, Set<String> bindingKeys, boolean durable) {
        Validate.notEmpty(topic, "topic is mandatory");
        Validate.notNull(subscriptionType, "subscription type is mandatory");

        try {
            //create virtual destination with routing keys
            String destinationTopic = topic;
            if (bindingKeys != null && !bindingKeys.isEmpty()) {
                destinationTopic = bindingKeys.stream()
                        .filter(StringUtils::isNotBlank)
                        .map(key -> topic + "." + key)
                        .collect(Collectors.joining( ","));
                destinationTopic = StringUtils.isNotBlank(destinationTopic) ? destinationTopic : topic;
            }

            MessageConsumer consumer;
            if (subscriptionType == QUEUE) {
                Session session = getSession(clientId);
                Queue queueDestination = session.createQueue(destinationTopic);
                consumer = session.createConsumer(queueDestination);
            } else  {
                Session session = getSession(clientId);
                Topic topicDestination = session.createTopic(topic);
                consumer = session.createDurableSubscriber(topicDestination, clientId);
            }

            LOG.debug("Created consumer on topic '{}'", topic);

            return consumer;
        } catch (JMSException e) {
            throw new ChannelException("Consumer creation failed with exception", e);
        }
    }

    public Destination createDestination(String topic, boolean isTopic) {
        try {
            Session session = getSession(null);
            return isTopic ? session.createTopic(topic) : session.createQueue(topic);
        } catch (JMSException e) {
            throw new ChannelException("Topic creation failed with exception", e);
        }
    }

    public Message createMessage(String body) {
        try {
            Session session = getSession(null);
            return session.createTextMessage(body);
        } catch (JMSException e) {
            throw new ChannelException("Message creation failed with exception", e);
        }
    }

    private Session getSession(String clientId) {
        try {
            return connectionManager.obtainConnection(clientId).createSession(false, Session.CLIENT_ACKNOWLEDGE);
        } catch (JMSException e) {
            throw new ChannelException("Session creation failed with exception", e);
        }
    }
}
