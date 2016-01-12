package io.github.tcdl.msb.impl;

import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.MessageHandlerResolver;
import io.github.tcdl.msb.api.message.Message;

import java.util.Optional;

/**
 * Trivial {@link MessageHandlerResolver} implementation that returns
 * a single single  {@link MessageHandler} by any incoming {@link Message}.
 */
public class SimpleMessageHandlerResolverImpl implements MessageHandlerResolver {

    private final MessageHandler messageHandler;

    private final String loggingName;

    public SimpleMessageHandlerResolverImpl(MessageHandler messageHandler, String loggingName) {
        this.messageHandler = messageHandler;
        this.loggingName = loggingName;
    }

    @Override
    public Optional<MessageHandler> resolveMessageHandler(Message message) {
        return Optional.of(messageHandler);
    }

    @Override
    public String getLoggingName() {
        return loggingName;
    }
}
