package io.github.tcdl.msb;

import io.github.tcdl.msb.api.message.Message;

import java.util.Optional;

/**
 * Implementations of this interface gives an ability to resolve {@link MessageHandler} by an incoming {@link Message}.
 */
public interface MessageHandlerResolver {

    /**
     * Resolve {@link MessageHandler} by an incoming {@link Message}.
     * @param message
     * @return
     */
    Optional<MessageHandler> resolveMessageHandler(Message message);

    /**
     * Get an arbitrary text name of the MessageHandlerResolver instance that
     * will be used used in log messages.
     * @return
     */
    String getLoggingName();
}
