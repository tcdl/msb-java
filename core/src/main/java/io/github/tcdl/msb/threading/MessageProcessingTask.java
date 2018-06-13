package io.github.tcdl.msb.threading;

import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.api.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Map;

/**
 * {@link MessageProcessingTask} wraps incoming message.
 */
public class MessageProcessingTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(MessageProcessingTask.class);

    final Message message;
    final MessageHandler messageHandler;
    final AcknowledgementHandlerInternal ackHandler;
    final Map<String, String> mdcLogContextMap;
    final boolean mdcLogCopy;

    public MessageProcessingTask( MessageHandler messageHandler, Message message,
                                     AcknowledgementHandlerInternal ackHandler) {
        this.message = message;
        this.messageHandler = messageHandler;
        this.ackHandler = ackHandler;
        this.mdcLogContextMap = MDC.getCopyOfContextMap();
        this.mdcLogCopy = mdcLogContextMap != null && !mdcLogContextMap.isEmpty();
    }

    /**
     * Passes the message to the configured handler and acknowledges it to AMQP broker.
     * IMPORTANT CAVEAT: This task is meant to be run in a thread pool so it should handle all its exceptions carefully. In particular it shouldn't
     * throw an exception (because it's going to be swallowed anyway and not printed)
     */
    @Override
    public void run() {
        if(mdcLogCopy) {
            MDC.setContextMap(mdcLogContextMap);
        }
        try {
            LOG.debug("[correlation id: {}] Starting message processing", message.getCorrelationId());
            messageHandler.handleMessage(message, ackHandler);
            LOG.debug("[correlation id: {}] Message has been processed", message.getCorrelationId());
            ackHandler.autoConfirm();
        } catch (Exception e) {
            LOG.error("[correlation id: {}] Failed to process message", message.getCorrelationId(), e);
            ackHandler.autoRetry();
        } catch (Error error) {
            LOG.error("[correlation id: {}] Failed to process message", message.getCorrelationId(), error);
            ackHandler.reportError(error);
        } finally {
            if(mdcLogCopy) {
                MDC.clear();
            }
        }
    }

    public Message getMessage() {
        return message;
    }

    public MessageHandler getMessageHandler() {
        return messageHandler;
    }

    public AcknowledgementHandlerInternal getAckHandler() {
        return ackHandler;
    }
}
