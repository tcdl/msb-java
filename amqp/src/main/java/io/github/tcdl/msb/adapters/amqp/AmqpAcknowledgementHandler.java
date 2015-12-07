package io.github.tcdl.msb.adapters.amqp;

import com.rabbitmq.client.AMQP;
import io.github.tcdl.msb.api.AcknowledgementHandler;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

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
    final boolean isMessageRedelivered;

    final AtomicBoolean acknowledgementSent = new AtomicBoolean(false);
    volatile boolean autoAcknowledgement = true;

    public AmqpAcknowledgementHandler(Channel channel, String consumerTag, long deliveryTag,
            boolean isMessageRedelivered) {
        super();
        this.channel = channel;
        this.consumerTag = consumerTag;
        this.deliveryTag = deliveryTag;
        this.isMessageRedelivered = isMessageRedelivered;
    }

    public boolean isAutoAcknowledgement() {
        return autoAcknowledgement;
    }

    public void setAutoAcknowledgement(boolean autoAcknowledgement) {
        this.autoAcknowledgement = autoAcknowledgement;
    }

    @Override
    public void confirmMessage() {
        executeAck("confirm", () -> {
            channel.basicAck(deliveryTag, false);
            LOG.debug(String.format("[consumer tag: %s] A message was confirmed", consumerTag));
        });
    }

    @Override
    public void retryMessage() {
        executeAck("requeue", () -> {
            if(!isMessageRedelivered) {
                channel.basicReject(deliveryTag, true);
                LOG.debug(String.format("[consumer tag: %s] A message was rejected with requeue", consumerTag));
            } else {
                channel.basicReject(deliveryTag, false);
                LOG.warn(String.format("[consumer tag: %s] Can't requeue message because it already was redelivered once, discarding it instead", consumerTag));
            }
        });
    }

    @Override
    public void rejectMessage() {
        executeAck("reject", () -> {
            channel.basicReject(deliveryTag, false);
            LOG.debug(String.format("[consumer tag: %s] A message was discarded", consumerTag));
        });
    }

    private void executeAck(String actionName, AckAction ackAction) {
        if (acknowledgementSent.compareAndSet(false, true)) {
            try {
                ackAction.perform();
            } catch (Exception e) {
                LOG.error(String.format("[consumer tag: %s] Got exception when trying to %s a message:", consumerTag, actionName), e);
            }
        } else {
            LOG.error(String.format(ACK_WAS_ALREADY_SENT, consumerTag));
        }
    }
    
    public void autoConfirm() {
        executeAutoAck(() -> {
            confirmMessage();
            LOG.debug(String.format("[consumer tag: %s] A message was automatically confirmed after message processing", consumerTag));
        });
    }

    public void autoReject() {
        executeAutoAck(() -> {
            rejectMessage();
            LOG.debug(String.format("[consumer tag: %s] A message was automatically rejected due to error during message processing", consumerTag));
        });
    }

    public void autoRetry() {
        executeAutoAck(() -> {
            retryMessage();
            LOG.debug(String.format("[consumer tag: %s] A message was automatically rejected (with a requeue attempt) due to error during message processing", consumerTag));
        });
    }

    private void executeAutoAck(AutoAckAction ackAction) {
        if (autoAcknowledgement && !acknowledgementSent.get()) {
            ackAction.perform();
        }
    }

    @FunctionalInterface
    private interface AckAction {
        void perform() throws Exception;
    }

    @FunctionalInterface
    private interface AutoAckAction {
        void perform();
    }

}
