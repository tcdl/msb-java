package io.github.tcdl.msb.adapters.activemq;

import io.github.tcdl.msb.api.SubscriptionType;
import io.github.tcdl.msb.api.exception.ChannelException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.github.tcdl.msb.api.SubscriptionType.QUEUE;

public class ActiveMQSessionManager {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQSessionManager.class);

    private static ActiveMQSessionManager instance;
    private ActiveMQConnectionManager connectionManager;
    private Map<String, Session> sessionsByClientId;

    private ActiveMQSessionManager(ActiveMQConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        this.sessionsByClientId = new ConcurrentHashMap<>();
    }

    static ActiveMQSessionManager instance(ActiveMQConnectionManager connectionManager) {
        if (instance == null) {
            instance = new ActiveMQSessionManager(connectionManager);
        }
        return instance;
    }

    public MessageProducer createProducer(String topic, SubscriptionType subscriptionType, String clientId) {
        Validate.notEmpty(topic, "topic is mandatory");
        Validate.notNull(subscriptionType, "subscription type is mandatory");

        try {
            // omit destination to specify it later during sending a message
            MessageProducer producer = getSession(clientId).createProducer(null);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
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

            Session session = getSession(clientId);
            Destination destination = createDestination(destinationTopic, subscriptionType, clientId);
            MessageConsumer consumer;
            if (subscriptionType == QUEUE) {
                consumer = session.createConsumer(destination);
            } else  {
                consumer = session.createDurableSubscriber((Topic)destination, clientId);
            }

            LOG.debug("Created consumer on topic '{}'", topic);

            return consumer;
        } catch (JMSException e) {
            throw new ChannelException("Consumer creation failed with exception", e);
        }
    }

    public Destination createDestination(String destinationTopic, SubscriptionType subscriptionType, String clientId) {
        Session session = getSession(clientId);
        try {
            return subscriptionType == QUEUE?
                    session.createQueue(destinationTopic):
                    session.createTopic(destinationTopic);
        } catch (JMSException e) {
            throw new ChannelException("Topic creation failed with exception", e);
        }
    }

    public Message createMessage(String body, String clientId) {
        try {
            Session session = getSession(clientId);
            return session.createTextMessage(body);
        } catch (JMSException e) {
            throw new ChannelException("Message creation failed with exception", e);
        }
    }

    private Session getSession(String clientId) {
        try {
            if (!sessionsByClientId.containsKey(clientId)) {
                Session session = connectionManager.obtainConnection(clientId).createSession(false, Session.CLIENT_ACKNOWLEDGE);
                sessionsByClientId.put(clientId, session);
            }
            return sessionsByClientId.get(clientId);
        } catch (JMSException e) {
            throw new ChannelException("Session creation failed with exception", e);
        }
    }
}
