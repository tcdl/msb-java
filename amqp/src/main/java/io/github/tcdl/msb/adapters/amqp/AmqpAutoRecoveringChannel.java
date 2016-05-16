package io.github.tcdl.msb.adapters.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Wrapper for {@link Channel} that support automatic re-initialization upon errors.
 */
public class AmqpAutoRecoveringChannel {
    private static final Logger LOG = LoggerFactory.getLogger(AmqpAutoRecoveringChannel.class);

    private AmqpConnectionManager connectionManager;
    private Channel channel;

    /**
     * Lock object used for 2 purposes:
     * 1. Prevent interleaving of basicPublish with confirmSelect
     * 2. Prevent interleaving of channel initialization and shutdown
     */
    private final Object lock = new Object();

    public AmqpAutoRecoveringChannel(AmqpConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete,
            Map<String, Object> arguments) throws IOException {
        Channel channel = obtainChannelForPublisherConfirms();
        return channel.exchangeDeclare(exchange, type, durable, autoDelete, arguments);
    }

    public void basicPublish(String exchange, String routingKey, AMQP.BasicProperties props, byte[] body) throws IOException {
        synchronized (lock) {
            Channel channel = obtainChannelForPublisherConfirms();
            channel.basicPublish(exchange, routingKey, props, body);
        }
    }

    private Channel obtainChannelForPublisherConfirms() throws IOException {
        synchronized (lock) {
            if (channel == null) {
                createChannelForPublisherConfirms(connectionManager);
            }
            return channel;
        }
    }

    private void createChannelForPublisherConfirms(AmqpConnectionManager connectionManager) throws IOException {

        if (channel != null) {
            channel = connectionManager.obtainConnection().createChannel(channel.getChannelNumber());
        } else {
            channel = connectionManager.obtainConnection().createChannel();
        }

        channel.confirmSelect();

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
            synchronized (lock) {
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
                    closeChannel(channel);
                    channel = null;
                }
            }
        });
    }

    private void closeChannel(Channel channel) {
        try {
            channel.abort();
        } catch (IOException e) {
            LOG.info("Error closing AMQP channel", e.getCause());
        }
    }
}
