package io.github.tcdl.msb.threading;

import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.api.AcknowledgementHandler;
import io.github.tcdl.msb.api.message.Message;

/**
 * This interface defines a way to invoke {@link MessageHandler} to process a {@link Message} received.
 */
public interface MessageHandlerInvoker {
    /**
     * Handle an incoming {@link Message} using {@link MessageHandler} provided. After an invocation attempt, one of
     * {@link AcknowledgementHandlerInternal} methods should be invoked (depending on result -
     * {@link AcknowledgementHandlerInternal#autoConfirm()} should be used the processing was successful):
     * it is required to call in order to confirm the message.
     * The method should always throw an exception when a message supplied can't be handled
     * so there will be no {@link MessageHandler#handleMessage(Message, AcknowledgementHandler)} invocation.
     *
     * @param messageHandler {@link MessageHandler} instance related to a {@link Message} to be processed.
     * @param message  {@link Message} to be processed.
     * @param acknowledgeHandler acknowledgement handler.
     * @throws RuntimeException when a message can't be handled.
     */
    void execute(MessageHandler messageHandler, Message message, AcknowledgementHandlerInternal acknowledgeHandler);

    /**
     * Perform cleanup on shutdown if required.
     */
    void shutdown();
}
