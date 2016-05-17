package io.github.tcdl.msb.threading;

import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.api.message.Message;

/**
 * Trivial {@link MessageHandlerInvoker} implementation that preforms a direct {@link MessageHandler} invocation
 * to process a {@link Message} received.
 */
public class DirectMessageHandlerInvoker implements MessageHandlerInvoker {

    @Override
    public void execute(MessageHandler messageHandler, Message message, AcknowledgementHandlerInternal acknowledgeHandler) {
        messageHandler.handleMessage(message, acknowledgeHandler);
        acknowledgeHandler.autoConfirm();
    }

    @Override
    public void shutdown() {

    }
}
