package io.github.tcdl.msb.adapters.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import io.github.tcdl.msb.api.exception.ChannelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Wrapper for {@link Channel} that support automatic re-initialization upon errors.
 */
public class LoggingAmqpChannel {
    private static final Logger LOG = LoggerFactory.getLogger(LoggingAmqpChannel.class);

    private AmqpConnectionManager connectionManager;
    private Channel channel;

    public static LoggingAmqpChannel instance(AmqpConnectionManager connectionManager){
        LoggingAmqpChannel loggingChannel = new LoggingAmqpChannel(connectionManager);
        loggingChannel.init();
        return loggingChannel;
    }

    private LoggingAmqpChannel(AmqpConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    /*
     * Initialization logic resides in this method instead of constructor in order to avoid 'this' reference escape
     * while object construction is not yet finished.
     */
    private void init() {
        try {
            this.channel = connectionManager.obtainConnection().createChannel();
        } catch (IOException e) {
            throw new ChannelException("Channel creation failed with exception", e);
        }

        channel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                LOG.debug("Processing publisher ack (deliveryTag = {}, multiple = {})", deliveryTag, multiple);
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                LOG.debug("Processing publisher nack (deliveryTag = {}, multiple = {})", deliveryTag, multiple);
            }
        });

        channel.addShutdownListener(cause -> {
            LOG.debug("Handling channel shutdown...");
            if (cause.isInitiatedByApplication()) {
                LOG.debug("Shutdown is initiated by application.");
            } else {
                LOG.error("Shutdown is NOT initiated by application.", cause);
            }
        });
    }

    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete, Map<String, Object> arguments) {
        LOG.debug("Declaring exchange. Name = [{}], type = [{}], durable = [{}], autoDelete = [{}], args = [{}].",
                exchange, type, durable, autoDelete, arguments);
        try {
            return channel.exchangeDeclare(exchange, type, durable, autoDelete, arguments);
        } catch (IOException e) {
            throw new ChannelException("exchange.declare call failed", e);
        }
    }

    public void basicPublish(String exchange, String routingKey, AMQP.BasicProperties props, byte[] body) {
        LOG.debug("Publishing message. Exchange name = [{}], routing key = [{}]", exchange, routingKey);
        try {
            channel.basicPublish(exchange, routingKey, props, body);
        } catch (IOException e) {
            throw new ChannelException("basic.publish call failed", e);
        }
    }
}
