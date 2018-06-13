package io.github.tcdl.msb.adapters.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownSignalException;
import io.github.tcdl.msb.acknowledge.AcknowledgementAdapter;

/**
 * AMQP acknowledgement implementation.
 */
public class AmqpAcknowledgementAdapter implements AcknowledgementAdapter {
    final Channel channel;
    final String identifier;
    final long deliveryTag;

    public AmqpAcknowledgementAdapter(Channel channel, String identifier, long deliveryTag) {
        this.channel = channel;
        this.identifier = identifier;
        this.deliveryTag = deliveryTag;
    }

    @Override
    public void confirm() throws Exception {
        channel.basicAck(deliveryTag, false);
    }

    @Override
    public void reject() throws Exception {
        channel.basicReject(deliveryTag, false);
    }

    @Override
    public void retry() throws Exception {
        channel.basicReject(deliveryTag, true);
    }

    @Override
    public void reportError(Throwable error) throws Exception {
        if (channel.isOpen()) {
            try {
                channel.close(AMQP.REPLY_SUCCESS, "Error: " + error.getClass().getCanonicalName() + "[" + error.getMessage() + "]");
            } catch (ShutdownSignalException e) {
                // do nothing
            }
        }
    }
}
