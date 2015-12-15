package io.github.tcdl.msb.adapters.amqp;

import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.api.exception.ChannelException;
import io.github.tcdl.msb.config.amqp.AmqpBrokerConfig;
import io.github.tcdl.msb.support.Utils;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang3.Validate;

import com.rabbitmq.client.Channel;

public class AmqpConsumerAdapter implements ConsumerAdapter {

    private String topic;
    private Channel channel;
    private String exchangeName;
    private String consumerTag;
    private AmqpBrokerConfig adapterConfig;

    /**
     * The constructor.
     * @param topic - a topic name associated with the adapter
     * @throws ChannelException if some problems during setup channel from RabbitMQ connection were occurred
     */
    public AmqpConsumerAdapter(String topic, AmqpBrokerConfig amqpBrokerConfig, AmqpConnectionManager connectionManager) {
        Validate.notNull(topic, "the 'topic' must not be null");

        this.topic = topic;
        this.exchangeName = topic;
        this.adapterConfig = amqpBrokerConfig;

        try {
            channel = connectionManager.obtainConnection().createChannel();
            channel.exchangeDeclare(exchangeName, "fanout", false /* durable */, true /* auto-delete */, null);
        } catch (IOException e) {
            throw new ChannelException("Failed to setup channel from ActiveMQ connection", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void subscribe(RawMessageHandler msgHandler) {
        String groupId = adapterConfig.getGroupId().orElse(Utils.generateId());
        boolean durable = adapterConfig.isDurable();
        int prefetchCount = adapterConfig.getPrefetchCount();

        String queueName = generateQueueName(topic, groupId, durable);

        try {
            channel.queueDeclare(queueName, durable /* durable */, false /* exclusive */, !durable /*auto-delete */, null);
            channel.basicQos(prefetchCount); // Don't accept more messages if we have any unacknowledged
            channel.queueBind(queueName, exchangeName, "");

            consumerTag = channel.basicConsume(queueName, false /* autoAck */, new AmqpMessageConsumer(channel, msgHandler, adapterConfig));
        } catch (IOException e) {
            throw new ChannelException(String.format("Failed to subscribe to topic %s", topic), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unsubscribe() {
        try {
            channel.basicCancel(consumerTag);
        } catch (IOException e) {
            throw new ChannelException(String.format("Failed to unsubscribe from topic %s", topic), e);
        }
    }

    /**
     * Generate topic name to get unique topics for different microservices
     * @param topic - topic name associated with the adapter
     * @param groupId - group service Id
     * @param durable - queue durability
     */
    private String generateQueueName(String topic, String groupId, boolean durable) {
        return topic + "." + groupId + "." + (durable ? "d" : "t");
    }
}
