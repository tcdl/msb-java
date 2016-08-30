package io.github.tcdl.msb.adapters.amqp;

import com.rabbitmq.client.MessageProperties;
import io.github.tcdl.msb.adapters.ProducerAdapter;
import io.github.tcdl.msb.api.MessageDestination;
import io.github.tcdl.msb.api.exception.ChannelException;
import io.github.tcdl.msb.config.amqp.AmqpBrokerConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.io.IOException;
import java.nio.charset.Charset;

public class AmqpProducerAdapter implements ProducerAdapter {
    private String exchangeName;
    private AmqpBrokerConfig amqpBrokerConfig;
    private AmqpAutoRecoveringChannel amqpAutoRecoveringChannel;

    /**
     * The constructor.
     * @param topic - a topic name associated with the adapter
     * @throws ChannelException if some problems during setup channel from RabbitMQ connection were occurred
     */
    public AmqpProducerAdapter(String topic, AmqpBrokerConfig amqpBrokerConfig, AmqpConnectionManager connectionManager) {
        this(topic, "fanout", amqpBrokerConfig, connectionManager);
    }

    public AmqpProducerAdapter(MessageDestination destination, AmqpBrokerConfig amqpBrokerConfig, AmqpConnectionManager connectionManager) {
        this(destination.getTopic(), "topic", amqpBrokerConfig, connectionManager);
    }

    public AmqpProducerAdapter(String topic, String exchangeType, AmqpBrokerConfig amqpBrokerConfig, AmqpConnectionManager connectionManager) {
        Validate.notNull(topic, "the 'topic' must not be null");

        this.exchangeName = topic;
        this.amqpBrokerConfig = amqpBrokerConfig;
        this.amqpAutoRecoveringChannel = new AmqpAutoRecoveringChannel(connectionManager);

        try {
            amqpAutoRecoveringChannel.exchangeDeclare(exchangeName, exchangeType, false /* durable */, true /* auto-delete */, null);
        } catch (IOException e) {
            throw new ChannelException("Failed to setup channel from ActiveMQ connection", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(String jsonMessage) {
        publish(jsonMessage, StringUtils.EMPTY);
    }

    @Override
    public void publish(String jsonMessage, String routingKey) {
        Validate.notNull(routingKey, "routing key is required");
        Charset charset = amqpBrokerConfig.getCharset();

        try {
            amqpAutoRecoveringChannel.basicPublish(exchangeName, routingKey, MessageProperties.PERSISTENT_BASIC, jsonMessage.getBytes(charset));
        } catch (IOException e) {
            throw new ChannelException(String.format("Failed to publish message '%s' into exchange '%s' with routing key '%s'", jsonMessage, exchangeName, routingKey), e);
        }
    }
}
