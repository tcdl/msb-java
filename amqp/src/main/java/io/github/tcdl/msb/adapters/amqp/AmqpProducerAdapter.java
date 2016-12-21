package io.github.tcdl.msb.adapters.amqp;

import com.rabbitmq.client.MessageProperties;
import io.github.tcdl.msb.adapters.ProducerAdapter;
import io.github.tcdl.msb.api.ExchangeType;
import io.github.tcdl.msb.api.exception.ChannelException;
import io.github.tcdl.msb.config.amqp.AmqpBrokerConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.nio.charset.Charset;

public class AmqpProducerAdapter implements ProducerAdapter {

    final String exchangeName;
    final AmqpBrokerConfig amqpBrokerConfig;
    final LoggingAmqpChannel channel;

    public AmqpProducerAdapter(String topic, ExchangeType exchangeType, AmqpBrokerConfig amqpBrokerConfig, AmqpConnectionManager connectionManager) {
        Validate.notNull(topic, "Topic is mandatory");
        Validate.notNull(exchangeType, "Exchange type is mandatory");
        Validate.notNull(amqpBrokerConfig, "Broker config is mandatory");
        Validate.notNull(exchangeType, "Connection manager is mandatory");

        this.exchangeName = topic;
        this.amqpBrokerConfig = amqpBrokerConfig;
        this.channel = LoggingAmqpChannel.instance(connectionManager);

        try {
            channel.exchangeDeclare(exchangeName, exchangeType.value(), false /* durable */, true /* auto-delete */, null);
        } catch (Exception e) {
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
            channel.basicPublish(exchangeName, routingKey, MessageProperties.PERSISTENT_BASIC, jsonMessage.getBytes(charset));
        } catch (Exception e) {
            throw new ChannelException(String.format("Failed to publish message '%s' into exchange '%s' with routing key '%s'", jsonMessage, exchangeName, routingKey), e);
        }
    }
}
