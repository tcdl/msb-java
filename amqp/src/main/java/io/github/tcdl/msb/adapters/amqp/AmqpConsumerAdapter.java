package io.github.tcdl.msb.adapters.amqp;

import com.rabbitmq.client.Channel;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.api.ExchangeType;
import io.github.tcdl.msb.api.exception.ChannelException;
import io.github.tcdl.msb.config.amqp.AmqpBrokerConfig;
import io.github.tcdl.msb.support.Utils;
import org.apache.commons.lang3.Validate;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;


public class AmqpConsumerAdapter implements ConsumerAdapter {

    private Channel channel;
    private final String exchangeName;
    private final Set<String> bindingKeys;
    private String consumerTag;
    private AmqpBrokerConfig adapterConfig;
    private boolean isResponseTopic = false;
    private Optional<String> currentQueueName = Optional.empty();

    public AmqpConsumerAdapter(String exchangeName, ExchangeType exchangeType, Set<String> bindingKeys, AmqpBrokerConfig amqpBrokerConfig,
                               AmqpConnectionManager connectionManager, boolean isResponseTopic) {
        Validate.notNull(exchangeName, "Exchange name is required");
        Validate.notNull(exchangeType, "Exchange type is required");
        Validate.notEmpty(bindingKeys, "At least one routing key is required");

        this.bindingKeys = bindingKeys;
        this.exchangeName = exchangeName;
        this.adapterConfig = amqpBrokerConfig;
        this.isResponseTopic = isResponseTopic;

        try {
            channel = connectionManager.obtainConnection().createChannel();
            channel.exchangeDeclare(exchangeName, exchangeType.value(), false /* durable */, true /* auto-delete */, null);
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
            for (String bindingKey : bindingKeys) {
                channel.queueBind(queueName, exchangeName, bindingKey);
            }
            consumerTag = channel.basicConsume(queueName, false /* autoAck */, new AmqpMessageConsumer(channel, msgHandler, adapterConfig));
            currentQueueName = Optional.of(queueName);
        } catch (IOException e) {
            throw new ChannelException(String.format("Failed to subscribe to topic %s with routing keys %s", exchangeName, bindingKeys), e);
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
            currentQueueName = Optional.empty();
        } catch (IOException e) {
            throw new ChannelException(String.format("Failed to unsubscribe from topic %s", exchangeName), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Long> messageCount() {
        return currentQueueName.map(queueName -> {
            try {
                return channel.messageCount(queueName);
            } catch (IOException e) {
                throw new ChannelException(String.format("Failed to fetch ready messages for topic %s and queue %s", exchangeName, queueName), e);
            }
        });
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
