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

    private static final String ACK_WAS_ALREADY_SENT = "[consumer tag: %s] Acknowledgement was already sent during message processing.";

    private static final Logger LOG = LoggerFactory.getLogger(AmqpAcknowledgementHandler.class);

    final Channel channel;
    final String consumerTag;
    final long deliveryTag;
    final boolean isRequeueRejectedMessages;

    final AtomicBoolean acknowledgementSent = new AtomicBoolean(false);
    boolean autoAcknowledgement = true;

    public AmqpAcknowledgementHandler(Channel channel, String consumerTag, long deliveryTag,
            boolean isRequeueRejectedMessages) {
        super();
        this.channel = channel;
        this.consumerTag = consumerTag;
        this.deliveryTag = deliveryTag;
        this.isRequeueRejectedMessages = isRequeueRejectedMessages;
    }

    public boolean isAutoAcknowledgement() {
        return autoAcknowledgement;
    }

    public void setAutoAcknowledgement(boolean autoAcknowledgement) {
        this.autoAcknowledgement = autoAcknowledgement;
    }

    @Override
    public void confirmMessage() {
        if (acknowledgementSent.compareAndSet(false, true)) {
            try {
                channel.basicAck(deliveryTag, false);
                LOG.debug(String.format("[consumer tag: %s] A message was confirmed", consumerTag));
            } catch (Exception e) {
                LOG.error(String.format("[consumer tag: %s] Got exception when trying to confirm a message:", consumerTag), e);
            }
        } else {
            LOG.error(String.format(ACK_WAS_ALREADY_SENT, consumerTag));
        }
    }

    @Override
    public void retryMessage() {
        if (acknowledgementSent.compareAndSet(false, true)) {
            try {
                channel.basicReject(deliveryTag, isRequeueRejectedMessages);
                LOG.debug(String.format("[consumer tag: %s] A message was rejected with requeue", consumerTag));
            } catch (Exception e) {
                LOG.error(String.format("[consumer tag: %s] Got exception when trying to reject with requeue a message:", consumerTag), e);
            }
        } else {
            LOG.error(String.format(ACK_WAS_ALREADY_SENT, consumerTag));
        }
    }

    @Override
    public void rejectMessage() {
        if (acknowledgementSent.compareAndSet(false, true)) {
            try {
                channel.basicReject(deliveryTag, false);
                LOG.debug(String.format("[consumer tag: %s] A message was discarded", consumerTag));
            } catch (Exception e) {
                LOG.error(String.format("[consumer tag: %s] Got exception when trying to discard a message:", consumerTag), e);
            }
        } else {
            LOG.error(String.format(ACK_WAS_ALREADY_SENT, consumerTag));
        }
    }
    
    public void autoConfirm() {
        if (autoAcknowledgement && !acknowledgementSent.get()) {
            confirmMessage();
            LOG.debug(String.format("[consumer tag: %s] A message was automatically confirmed after message processing", consumerTag));
        }
    }

    public void autoReject() {
        if (autoAcknowledgement && !acknowledgementSent.get()) {
            rejectMessage();
            LOG.debug(String.format("[consumer tag: %s] A message was automatically rejected due to error during message processing", consumerTag));
        }
    }

}
