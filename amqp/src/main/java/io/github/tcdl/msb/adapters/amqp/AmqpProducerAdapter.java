package io.github.tcdl.msb.adapters.amqp;

import com.rabbitmq.client.MessageProperties;
import io.github.tcdl.msb.adapters.ProducerAdapter;
import io.github.tcdl.msb.api.ExchangeType;
import io.github.tcdl.msb.api.exception.ChannelException;
import io.github.tcdl.msb.config.amqp.AmqpBrokerConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

public class AmqpProducerAdapter implements ProducerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(AmqpProducerAdapter.class);
    private static final String ERROR_MESSAGE_TEMPLATE = "Failed to publish message into exchange '%s' with routing key '%s'";

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
            LOG.debug(ERROR_MESSAGE_TEMPLATE, exchangeName, routingKey);
            LOG.trace("Message: {}", jsonMessage);
            throw new ChannelException(String.format(ERROR_MESSAGE_TEMPLATE, exchangeName, routingKey), e);
        }
    }
}
