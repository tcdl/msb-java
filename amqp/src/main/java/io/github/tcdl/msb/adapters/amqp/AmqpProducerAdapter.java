package io.github.tcdl.msb.adapters.amqp;

import com.rabbitmq.client.MessageProperties;
import io.github.tcdl.msb.adapters.ProducerAdapter;
import io.github.tcdl.msb.api.exception.ChannelException;
import io.github.tcdl.msb.config.amqp.AmqpBrokerConfig;
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
        Validate.notNull(topic, "the 'topic' must not be null");

        this.exchangeName = topic;
        this.amqpBrokerConfig = amqpBrokerConfig;
        this.amqpAutoRecoveringChannel = new AmqpAutoRecoveringChannel(connectionManager);

        try {
            amqpAutoRecoveringChannel.exchangeDeclare(exchangeName, "fanout", false /* durable */, true /* auto-delete */, null);
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
            Charset charset = amqpBrokerConfig.getCharset();
            amqpAutoRecoveringChannel.basicPublish(exchangeName, "" /* routing key */, MessageProperties.PERSISTENT_BASIC, jsonMessage.getBytes(charset));
        } catch (IOException e) {
            throw new ChannelException(String.format("Failed to publish message '%s' into exchange '%s'", jsonMessage, exchangeName), e);
        }
    }
}
