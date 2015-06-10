package io.github.tcdl.adapters;

import io.github.tcdl.exception.ChannelException;

/**
 * {@link ProducerAdapter} allows to produce messages to message bus. One adapter instance is associated with specific topic.
 *
 * Implementations should allow to connect to and use different kinds of buses (like AMQP, JMS, etc)
 */
public interface ProducerAdapter {

    /**
     * Publishes the message to the associated topic
     */
    void publish(String jsonMessage) throws ChannelException;
}
