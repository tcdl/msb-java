package io.github.tcdl.msb.adapters.amqp;

import com.rabbitmq.client.Channel;
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
}
