package io.github.tcdl.msb.adapters.amqp;

import io.github.tcdl.msb.api.AcknowledgementHandler;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;

/**
 * {@link AmqpAcknowledgementHandler} provides acknowledgement for AMQP broker. 
 * Used from {@link AmqpMessageConsumer}.
 */

public class AmqpAcknowledgementHandler implements AcknowledgementHandler {

    private static final String ACK_WAS_ALREADY_SENT = "Acknowledgement was already sent during message processing.";

    private static final Logger LOG = LoggerFactory.getLogger(AmqpAcknowledgementHandler.class);

    final Channel channel;
    final String consumerTag;
    final long deliveryTag;
    final boolean isRequeueRejectedMessages;

    final AtomicBoolean acknowledgementSent = new AtomicBoolean(false);

    public AmqpAcknowledgementHandler(Channel channel, String consumerTag, long deliveryTag,
            boolean isRequeueRejectedMessages) {
        super();
        this.channel = channel;
        this.consumerTag = consumerTag;
        this.deliveryTag = deliveryTag;
        this.isRequeueRejectedMessages = isRequeueRejectedMessages;
    }

    @Override
    public void confirmMessage() {
        if (acknowledgementSent.compareAndSet(false, true)) {
            try {
                channel.basicAck(deliveryTag, false);
            } catch (Exception e) {
                LOG.error(String.format("[consumer tag: %s] Got exception when trying to confirm a message:", consumerTag), e);
            }
        } else {
            LOG.warn(ACK_WAS_ALREADY_SENT);
        }
    }

    @Override
    public void rejectMessage() {
        if (acknowledgementSent.compareAndSet(false, true)) {
            try {
                channel.basicReject(deliveryTag, isRequeueRejectedMessages);
            } catch (Exception e) {
                LOG.error(String.format("[consumer tag: %s] Got exception when trying to reject a message:", consumerTag), e);
            }
        } else {
            LOG.warn(ACK_WAS_ALREADY_SENT);
        }
    }

    public void autoConfirm() {
        if (!acknowledgementSent.get()) {
            confirmMessage();
        }
    }

    public void autoReject() {
        if (!acknowledgementSent.get()) {
            rejectMessage();
        }
    }

}
