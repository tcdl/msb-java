package io.github.tcdl.adapters.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import io.github.tcdl.adapters.ProducerAdapter;
import io.github.tcdl.api.exception.ChannelException;
import org.apache.commons.lang3.Validate;

import java.io.IOException;

public class AmqpProducerAdapter implements ProducerAdapter {
    private Channel channel;
    private String exchangeName;

    /**
     * The constructor.
     * @param topic - a topic name associated with the adapter
     * @throws ChannelException if some problems during setup channel from RabbitMQ connection were occurred
     */
    public AmqpProducerAdapter(String topic, AmqpConnectionManager connectionManager) {
        Validate.notNull(topic, "the 'topic' must not be null");

        this.exchangeName = topic;

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
    public void publish(String jsonMessage) {
        try {
            channel.basicPublish(exchangeName, "" /* routing key */, MessageProperties.PERSISTENT_BASIC, jsonMessage.getBytes());
        } catch (IOException e) {
            throw new ChannelException(String.format("Failed to publish message '%s' into exchange '%s'", jsonMessage, exchangeName), e);
        }
    }
}
