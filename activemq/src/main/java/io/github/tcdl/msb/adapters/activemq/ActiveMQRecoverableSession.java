package io.github.tcdl.msb.adapters.activemq;

import io.github.tcdl.msb.api.SubscriptionType;
import io.github.tcdl.msb.api.exception.ChannelException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.Set;

import static io.github.tcdl.msb.api.SubscriptionType.QUEUE;
import static io.github.tcdl.msb.api.SubscriptionType.TOPIC;

public class ActiveMQRecoverableSession {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQRecoverableSession.class);

    private static final String ROUTING_KEY = "routingKey";

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
            Session session = openSession();
            Destination destinationTopic = createDestination(session, topic, subscriptionType == TOPIC);
            MessageProducer producer = session.createProducer(destinationTopic);
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
            if (subscriptionType == QUEUE && durable) {
                throw new ChannelException("Durable consumers are not supported for queue subscription", new IllegalArgumentException(subscriptionType.name()));
            }

            Session session = openSession();
            Destination destinationTopic = createDestination(session, topic, subscriptionType == TOPIC);
            String messageSelector = bindingKeys == null || bindingKeys.isEmpty() || (bindingKeys.size() == 1 && StringUtils.isBlank(bindingKeys.iterator().next()))?
                    null: String.format(ROUTING_KEY + " IN('%s')", StringUtils.join(bindingKeys.toArray(), "','"));

            MessageConsumer consumer;
            if (subscriptionType == QUEUE || !durable) {
                consumer = session.createConsumer(destinationTopic, messageSelector);
            } else  {
                consumer = session.createDurableSubscriber((Topic)destinationTopic, clientId, messageSelector, false);
            }

            LOG.debug("Created consumer on topic '{}'", topic);

            return consumer;
        } catch (JMSException e) {
            throw new ChannelException("Consumer creation failed with exception", e);
        }
    }

    public Message createMessage(String body, String routingKey) {
        try {
            Session session = openSession();
            TextMessage message = session.createTextMessage(body);
            if (StringUtils.isNotBlank(routingKey)) {
                message.setStringProperty(ROUTING_KEY, routingKey);
            }
            return message;
        } catch (JMSException e) {
            throw new ChannelException("Message creation failed with exception", e);
        }
    }

    private Session openSession() {
        try {
            return connectionManager.obtainConnection().createSession(false, Session.CLIENT_ACKNOWLEDGE);
        } catch (JMSException e) {
            throw new ChannelException("Session creation failed with exception", e);
        }
    }

    private Destination createDestination(Session session, String topic, boolean isTopic) {
        try {
            return isTopic ? session.createTopic(topic) : session.createQueue(topic);
        } catch (JMSException e) {
            throw new ChannelException("Topic creation failed with exception", e);
        }
    }
}
