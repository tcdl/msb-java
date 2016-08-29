package io.github.tcdl.msb.adapters.amqp;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.api.MessageDestination;
import io.github.tcdl.msb.api.exception.ChannelException;
import io.github.tcdl.msb.config.amqp.AmqpBrokerConfig;
import io.github.tcdl.msb.support.Utils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpConsumerAdapter implements ConsumerAdapter {

    private Channel channel;
    private final String exchangeName;
    private final Set<String> routingKeys;
    private String consumerTag;
    private AmqpBrokerConfig adapterConfig;
    private boolean isResponseTopic = false;

    /**
     * The constructor.
     *
     * @param exchangeName - an exchange name associated with the adapter
     * @throws ChannelException if adapter failed to create channel and declare exchange
     */
    public AmqpConsumerAdapter(String exchangeName, AmqpBrokerConfig amqpBrokerConfig, AmqpConnectionManager connectionManager, boolean isResponseTopic) {
        this(exchangeName, "fanout", Collections.singleton(StringUtils.EMPTY), amqpBrokerConfig, connectionManager, isResponseTopic);
    }

    public AmqpConsumerAdapter(String topic, Set<String> routingKeys, AmqpBrokerConfig amqpBrokerConfig, AmqpConnectionManager connectionManager) {
        this(topic, "topic", routingKeys, amqpBrokerConfig, connectionManager, false);
    }

    private AmqpConsumerAdapter(String exchangeName, String exchangeType, Set<String> routingKeys, AmqpBrokerConfig amqpBrokerConfig,
                                AmqpConnectionManager connectionManager, boolean isResponseTopic) {

        Validate.notNull(exchangeName, "Exchange name is required");
        Validate.notEmpty(routingKeys, "At least one routing key is required");

        this.routingKeys = routingKeys;
        this.exchangeName = exchangeName;
        this.adapterConfig = amqpBrokerConfig;
        this.isResponseTopic = isResponseTopic;

        try {
            channel = connectionManager.obtainConnection().createChannel();
            channel.exchangeDeclare(exchangeName, exchangeType, false /* durable */, true /* auto-delete */, null);
        } catch (IOException e) {
            throw new ChannelException("Failed to setup channel", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void subscribe(RawMessageHandler msgHandler) {
        String groupId = adapterConfig.getGroupId().orElse(Utils.generateId());
        boolean durable = isDurable();
        int prefetchCount = adapterConfig.getPrefetchCount();

        String queueName = generateQueueName(exchangeName, groupId, durable);

        try {
            channel.queueDeclare(queueName, durable /* durable */, false /* exclusive */, !durable /*auto-delete */, null);
            channel.basicQos(prefetchCount); // Don't accept more messages if we have any unacknowledged
            for(String routingKey: routingKeys) {
                channel.queueBind(queueName, exchangeName, routingKey);
            }
            consumerTag = channel.basicConsume(queueName, false /* autoAck */, new AmqpMessageConsumer(channel, msgHandler, adapterConfig));
        } catch (IOException e) {
            throw new ChannelException(String.format("Failed to subscribe to topic %s with routing keys %s", exchangeName, routingKeys), e);
        }
    }

    protected boolean isDurable() {
        if (isResponseTopic) {
            //response topic is always auto-delete and not durable
            return false;
        }
        return adapterConfig.isDurable();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unsubscribe() {
        try {
            channel.basicCancel(consumerTag);
        } catch (IOException e) {
            throw new ChannelException(String.format("Failed to unsubscribe from topic %s", exchangeName), e);
        }
    }

    /**
     * Generate topic name to get unique topics for different microservices
     *
     * @param topic   - topic name associated with the adapter
     * @param groupId - group service Id
     * @param durable - queue durability
     */
    private String generateQueueName(String topic, String groupId, boolean durable) {
        return topic + "." + groupId + "." + (durable ? "d" : "t");
    }
}
