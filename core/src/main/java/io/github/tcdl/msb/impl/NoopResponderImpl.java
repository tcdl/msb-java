package io.github.tcdl.msb.impl;

import io.github.tcdl.msb.api.Responder;
import io.github.tcdl.msb.api.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dummy implementation of {@link Responder} that is not able to send responses and acks.
 */
public class NoopResponderImpl implements Responder {
    private static final Logger LOG = LoggerFactory.getLogger(NoopResponderImpl.class);

    private Message originalMessage;

    public NoopResponderImpl(Message originalMessage) {
        this.originalMessage = originalMessage;
    }

    /** {@inheritDoc} */
    @Override
    public void sendAck(Integer timeoutMs, Integer responsesRemaining) {
        LOG.error("Cannot send ack because response topic is unknown. Incoming message: {}", originalMessage);
    }

    /** {@inheritDoc} */
    @Override
    public void send(Object responsePayload) {
        LOG.error("Cannot send response because response topic is unknown. Incoming message: {}", originalMessage);
    }

    /** {@inheritDoc} */
    @Override
    public Message getOriginalMessage() {
        return originalMessage;
    }
}
