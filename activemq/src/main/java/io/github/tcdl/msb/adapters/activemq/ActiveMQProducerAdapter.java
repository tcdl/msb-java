package io.github.tcdl.msb.adapters.activemq;

import io.github.tcdl.msb.adapters.ProducerAdapter;
import io.github.tcdl.msb.api.SubscriptionType;
import io.github.tcdl.msb.api.exception.ChannelException;
import io.github.tcdl.msb.config.activemq.ActiveMQBrokerConfig;
import joptsimple.internal.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;

public class ActiveMQProducerAdapter implements ProducerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQProducerAdapter.class);

    private static final String VIRTUAL_DESTINATION_PREFIX = "VirtualTopic.";
    private static final String PRODUCER_ID_PATTERN = "Producer.%s";
    private static final String ERROR_MESSAGE_TEMPLATE = "Failed to publish message to topic '%s' with routing key '%s'";

    private String physicalTopic;
    private SubscriptionType subscriptionType;
    private boolean durable;
    private ActiveMQSessionManager sessionManager;
    private MessageProducer producer;

    ActiveMQProducerAdapter(String topic, SubscriptionType subscriptionType, ActiveMQBrokerConfig brokerConfig, ActiveMQConnectionManager connectionManager) {
        Validate.notNull(topic, "Topic is mandatory");
        Validate.notNull(subscriptionType, "Subscription type is mandatory");
        Validate.notNull(brokerConfig, "Broker config is mandatory");
        Validate.notNull(connectionManager, "Connection manager is mandatory");

        this.physicalTopic = formatTopic(topic, subscriptionType, brokerConfig.isDurable());
        this.subscriptionType = subscriptionType;
        this.durable = brokerConfig.isDurable();

        try {
            String clientId = String.format(PRODUCER_ID_PATTERN, physicalTopic);
            this.sessionManager = ActiveMQSessionManager.instance(connectionManager);
            this.producer = sessionManager.createProducer(physicalTopic, subscriptionType, clientId);
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
            String clientId = String.format(PRODUCER_ID_PATTERN, physicalTopic);
            Message message = sessionManager.createMessage(jsonMessage, clientId);
            LOG.debug("Publishing message. Topic name = [{}], routing key = [{}]", physicalTopic, routingKey);

            //create virtual destination with routing key
            String destinationTopic = this.physicalTopic;
            if (StringUtils.isNotBlank(routingKey)) {
                destinationTopic += "." + routingKey;
            }

            Destination destination = sessionManager.createDestination(physicalTopic, subscriptionType, clientId);
            producer.send(destination, message);
        } catch (Exception e) {
            LOG.error(ERROR_MESSAGE_TEMPLATE, physicalTopic, routingKey);
            LOG.trace("Message: {}", jsonMessage);
            throw new ChannelException(String.format(ERROR_MESSAGE_TEMPLATE, physicalTopic, routingKey), e);
        }
    }

    private String formatTopic(String topic, SubscriptionType subscriptionType, boolean durable) {
        return VIRTUAL_DESTINATION_PREFIX + topic;
    }
}
