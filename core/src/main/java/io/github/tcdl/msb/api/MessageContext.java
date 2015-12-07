package io.github.tcdl.msb.api;

import io.github.tcdl.msb.api.message.Message;

/**
 * Provides access to Message Context.
 */
public interface MessageContext {
    /**
     * @return AcknowledgementHandler for explicit confirm/reject incoming messages
     */
    AcknowledgementHandler getAcknowledgementHandler();

    /**
     * @return original message to send a response to
     */
    Message getOriginalMessage();
}
