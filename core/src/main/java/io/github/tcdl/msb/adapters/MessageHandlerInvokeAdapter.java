package io.github.tcdl.msb.adapters;

import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.api.message.Message;

/**
 * Interface that defines a way to invoke {@link MessageHandler} to process a {@link Message} received.
 */
public interface MessageHandlerInvokeAdapter {
    /**
     * Handle an incoming {@link Message} using {@link MessageHandler} provided.
     * @param messageHandler {@link MessageHandler} instance related to a {@link Message} to be processed.
     * @param message  {@link Message} to be processed.
     * @param acknowledgeHandler acknowledgement handler.
     */
    void execute(MessageHandler messageHandler, Message message, AcknowledgementHandlerInternal acknowledgeHandler);
}
