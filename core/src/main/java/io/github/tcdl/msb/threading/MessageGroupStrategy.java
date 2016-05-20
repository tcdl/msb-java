package io.github.tcdl.msb.threading;

import io.github.tcdl.msb.api.message.Message;

import java.util.Optional;

/**
 * Implementations of this interface define a way to resolve a message group by a message. Messages
 * with the same message group will be executed by {@link GroupedExecutorBasedMessageHandlerInvoker}
 * one after another (in a single-threaded mode)
 * while messages with different message groups could be executed in parallel.
 */
@FunctionalInterface
public interface MessageGroupStrategy {
    /**
     * Resolve message group by a message. Message group identifier is any integer. If the message group
     * can't be resolved for a particular message, {@link Optional#empty()} should be returned. In this
     * case an execution thread will be selected randomly.
     * @param message
     * @return
     */
    Optional<Integer> getMessageGroupId(Message message);
}