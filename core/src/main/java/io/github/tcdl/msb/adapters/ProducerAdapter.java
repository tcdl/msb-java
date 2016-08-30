package io.github.tcdl.msb.adapters;

import io.github.tcdl.msb.api.exception.ChannelException;

/**
 * {@link ProducerAdapter} allows to produce messages to message bus. One adapter instance is associated with specific topic.
 *
 * Implementations should allow to connect to and use different kinds of buses (like AMQP, JMS, etc)
 */
public interface ProducerAdapter {

    /**
     * Publishes the message to the associated topic
     *
     * @param jsonMessage message to publish in JSON format
     * @throws ChannelException if some problems during publishing message to Broker were occurred
     */
    void publish(String jsonMessage);

    /**
     * Publishes the message to the associated topic with specified routing key
     *
     * @param jsonMessage message to publish in JSON format
     * @param routingKey non null String of max length 255 bytes to be used for message routing
     */
    void publish(String jsonMessage, String routingKey);
}
