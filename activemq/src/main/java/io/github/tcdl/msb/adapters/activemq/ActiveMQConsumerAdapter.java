package io.github.tcdl.msb.adapters.activemq;

import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.api.SubscriptionType;
import io.github.tcdl.msb.api.exception.ChannelException;
import io.github.tcdl.msb.config.activemq.ActiveMQBrokerConfig;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import java.util.Optional;
import java.util.Set;

public class ActiveMQConsumerAdapter implements ConsumerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQConsumerAdapter.class);

    private static final String VIRTUAL_DESTINATION_PREFIX = "VirtualTopic.";
    private static final String CONSUMER_TOPIC_PATTERN = "Consumer.%s.%s";

    private final String physicalTopic;
    private final String groupId;
    private final SubscriptionType subscriptionType;
    private final Set<String> bindingKeys;
    private final ActiveMQBrokerConfig brokerConfig;
    private boolean isResponseTopic = false;

    private final ActiveMQRecoverableSession session;
    private MessageConsumer consumer;


    public ActiveMQConsumerAdapter(String topic, SubscriptionType subscriptionType, Set<String> bindingKeys, ActiveMQBrokerConfig brokerConfig,
                                   ActiveMQConnectionManager connectionManager, boolean isResponseTopic) {
        Validate.notNull(topic, "Topic name is required");
        Validate.notNull(subscriptionType, "Subscription type is required");

        this.groupId = brokerConfig.getGroupId().orElse("msb");
        this.physicalTopic = String.format(CONSUMER_TOPIC_PATTERN, groupId, VIRTUAL_DESTINATION_PREFIX + topic);
        this.subscriptionType = subscriptionType;
        this.bindingKeys = bindingKeys;
        this.brokerConfig = brokerConfig;
        this.isResponseTopic = isResponseTopic;
        this.session = ActiveMQRecoverableSession.instance(connectionManager);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void subscribe(RawMessageHandler msgHandler) {
        try {
            ActiveMQMessageConsumer messageConsumer = new ActiveMQMessageConsumer(msgHandler);
            String clientId = generateClientId(physicalTopic, groupId);
            consumer = session.createConsumer(physicalTopic, subscriptionType, clientId, bindingKeys, isDurable());
            consumer.setMessageListener(messageConsumer::handlerMessage);
        } catch (JMSException e) {
            throw new ChannelException(String.format("Failed to subscribe to topic %s with binding keys %s", physicalTopic, bindingKeys), e);
        }
    }

    private boolean isDurable() {
        return !isResponseTopic && brokerConfig.isDurable();
    }

    private String generateClientId(String topic, String groupId) {
        return String.format(CONSUMER_TOPIC_PATTERN, groupId, topic);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unsubscribe() {
        try {
            consumer.close();
        } catch (JMSException e) {
            throw new ChannelException(String.format("Failed to unsubscribe from topic %s", physicalTopic), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Long> messageCount() {
        throw new UnsupportedOperationException("Message count metric is not supported");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Boolean> isConnected() {
        throw new UnsupportedOperationException("IsConnected is not supported");
    }
}
