package io.github.tcdl.msb.adapters;

import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.api.message.Message;

/**
 * Interface that defines a way to invoke {@link MessageHandler} to process a {@link Message} received.
 */
public interface MessageHandlerInvokeStrategy {
    /**
     * Handle an incoming {@link Message} using {@link MessageHandler} provided. After an invocation attempt, one of
     * {@link AcknowledgementHandlerInternal} methods should be invoked (depending on result -
     * {@link AcknowledgementHandlerInternal#autoConfirm()} should be used the processing was successful):
     * it is required to call in order to confirm the message.
     * @param messageHandler {@link MessageHandler} instance related to a {@link Message} to be processed.
     * @param message  {@link Message} to be processed.
     * @param acknowledgeHandler acknowledgement handler.
     */
    void execute(MessageHandler messageHandler, Message message, AcknowledgementHandlerInternal acknowledgeHandler);
}
