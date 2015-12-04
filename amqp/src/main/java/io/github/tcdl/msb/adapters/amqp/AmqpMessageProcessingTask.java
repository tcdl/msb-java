package io.github.tcdl.msb.adapters.amqp;

import io.github.tcdl.msb.adapters.ConsumerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AmqpMessageProcessingTask} wraps incoming message. This task is put into message processing thread pool (see {@link AmqpMessageConsumer}).
 */
public class AmqpMessageProcessingTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(AmqpMessageProcessingTask.class);

    final String consumerTag;
    final String body;
    final ConsumerAdapter.RawMessageHandler msgHandler;
    final AmqpAcknowledgementHandler ackHandler;

    public AmqpMessageProcessingTask(String consumerTag, String body, ConsumerAdapter.RawMessageHandler msgHandler, 
            AmqpAcknowledgementHandler ackHandler) {
        this.consumerTag = consumerTag;
        this.body = body;
        this.msgHandler = msgHandler;
        this.ackHandler = ackHandler;
    }

    /**
     * Passes the message to the configured handler and acknowledges it to AMQP broker.
     * IMPORTANT CAVEAT: This task is meant to be run in a thread pool so it should handle all its exceptions carefully. In particular it shouldn't
     * throw an exception (because it's going to be swallowed anyway and not printed)
     */
    @Override
    public void run() {
        try {
            LOG.debug(String.format("[consumer tag: %s] Starting message processing: %s", consumerTag, body));
            msgHandler.onMessage(body, ackHandler);
            LOG.debug(String.format("[consumer tag: %s] Message has been processed: %s", consumerTag, body));
            ackHandler.autoConfirm();
        } catch (Exception e) {
            LOG.error(String.format("[consumer tag: %s] Failed to process message %s", consumerTag, body), e);
            ackHandler.autoReject();
        }
    }
    
}
