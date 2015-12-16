package io.github.tcdl.msb.impl;

import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.adapters.MessageHandlerInvokeAdapter;
import io.github.tcdl.msb.api.message.Message;

/**
 * Trivial {@link MessageHandlerInvokeAdapter} implementation that preforms a direct {@link MessageHandler} invocation
 * to process a {@link Message} received.
 */
public class SimpleMessageHandlerInvokeAdapterImpl implements MessageHandlerInvokeAdapter {
    @Override
    public void execute(MessageHandler messageHandler, Message message, AcknowledgementHandlerInternal acknowledgeHandler) {
        messageHandler.handleMessage(message, acknowledgeHandler);
    }
}
