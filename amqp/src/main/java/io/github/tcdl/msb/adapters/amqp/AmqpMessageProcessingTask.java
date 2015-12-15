package io.github.tcdl.msb.adapters.amqp;

import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.api.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AmqpMessageProcessingTask} wraps incoming message. This task is put into message processing thread pool (see {@link AmqpMessageConsumer}).
 */
public class AmqpMessageProcessingTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(AmqpMessageProcessingTask.class);

    final Message message;
    final MessageHandler messageHandler;
    final AcknowledgementHandlerInternal ackHandler;

    public AmqpMessageProcessingTask( MessageHandler messageHandler, Message message,
                                     AcknowledgementHandlerInternal ackHandler) {
        this.message = message;
        this.messageHandler = messageHandler;
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
            LOG.debug(String.format("[correlation id: %s] Starting message processing", message.getCorrelationId()));
            messageHandler.handleMessage(message, ackHandler);
            LOG.debug(String.format("[correlation id: %s] Message has been processed", message.getCorrelationId()));
            ackHandler.autoConfirm();
        } catch (Exception e) {
            LOG.error(String.format("[correlation id: %s] Failed to process message", message.getCorrelationId()), e);
            ackHandler.autoRetry();
        }
    }
    
}
