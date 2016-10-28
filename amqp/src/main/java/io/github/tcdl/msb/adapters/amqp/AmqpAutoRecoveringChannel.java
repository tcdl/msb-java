package io.github.tcdl.msb.adapters.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import io.github.tcdl.msb.api.exception.ChannelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Wrapper for {@link Channel} that support automatic re-initialization upon errors.
 */
public class AmqpAutoRecoveringChannel {
    private static final Logger LOG = LoggerFactory.getLogger(AmqpAutoRecoveringChannel.class);

    private AmqpConnectionManager connectionManager;

    /**
     * Lock object used for 2 purposes:
     * 1. Prevent interleaving of basicPublish with confirmSelect
     * 2. Prevent interleaving of channel initialization and shutdown
     */
    private final AtomicReference<Channel> channelReference = new AtomicReference<>();

    public AmqpAutoRecoveringChannel(AmqpConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete, Map<String, Object> arguments) {
        LOG.debug("Declaring exchange. Name = [{}], type = [{}], durable = [{}], autoDelete = [{}], args = [{}].",
                exchange, type, durable, autoDelete, arguments);

        return atomicOperationOnChannel(channel -> {
            try {
                return channel.exchangeDeclare(exchange, type, durable, autoDelete, arguments);
            } catch (IOException e) {
                throw new ChannelException("exchange.declare call failed", e);
            }
        });
    }

    public void basicPublish(String exchange, String routingKey, AMQP.BasicProperties props, byte[] body) {
        LOG.debug("Publishing message. Exchange name = [{}], routing key = [{}]", exchange, routingKey);
        atomicOperationOnChannel(channel -> {
            try {
                channel.basicPublish(exchange, routingKey, props, body);
            } catch (IOException e) {
                throw new ChannelException("basic.publish call failed", e);
            }
        });
    }

    private <T> T atomicOperationOnChannel(Function<Channel, T> operation) {
        synchronized (channelReference) {
            Channel channel = getOpenChannel();
            return operation.apply(channel);
        }
    }

    private void atomicOperationOnChannel(Consumer<Channel> operation) {
        synchronized (channelReference) {
            Channel channel = getOpenChannel();
            operation.accept(channel);
        }
    }

    private Channel getOpenChannel() {
        return channelReference.updateAndGet(currentChannel -> {
            if (currentChannel == null) {
                return newChannelWithPublisherConfirms();
            } else if (!currentChannel.isOpen()) {
                closeChannel(currentChannel);
                return newChannelWithPublisherConfirms();
            } else {
                return currentChannel;
            }
        });
    }

    private Channel newChannelWithPublisherConfirms() {
        Channel newChannel;
        try {
            newChannel = connectionManager.obtainConnection().createChannel();
            newChannel.confirmSelect();
        } catch (IOException e) {
            throw new ChannelException("Channel creation failed", e);
        }

        newChannel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                LOG.debug("Processing publisher ack (deliveryTag = {}, multiple = {})", deliveryTag, multiple);
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                LOG.debug("Processing publisher nack (deliveryTag = {}, multiple = {})", deliveryTag, multiple);
            }
        });

        newChannel.addShutdownListener(cause -> {
            LOG.debug("Handling channel shutdown...");
            if (cause.isInitiatedByApplication()) {
                LOG.debug("Shutdown is initiated by application. Ignoring it.");
            } else {
                LOG.error("Shutdown is NOT initiated by application. Resetting the channel.", cause);
                /*
                We cannot re-initialize channel here directly because ShutdownListener callbacks run in the connection's thread,
                so the call to createChannel causes a deadlock since it blocks waiting for a response (whilst the connection's thread
                is stuck executing the listener).
                 */

                Channel oldChannel = channelReference.getAndSet(null);
                closeChannel(oldChannel);
            }
        });

        return newChannel;
    }

    private void closeChannel(Channel channel) {
        try {
            LOG.debug("Closing channel. Channel number = [{}]", channel.getChannelNumber());
            channel.abort();
            LOG.debug("Channel [{}] closed.", channel.getChannelNumber());
        } catch (IOException e) {
            LOG.info("Error closing AMQP channel", e.getCause());
        }
    }
}
