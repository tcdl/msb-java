package io.github.tcdl.adapters;

import io.github.tcdl.exception.ChannelException;

/**
 * {@link ConsumerAdapter} allows to receive messages from message bus. One adapter instance is associated with specific topic.
 *
 * Implementations should allow to connect to and use different kinds of buses (like AMQP, JMS, etc)
 */
public interface ConsumerAdapter {
    /**
     * Subscribes the given message handler to the associated topic
     * @param onMessageHandler this handler is invoked once message arrives on the topic.
     *                         THE IMPLEMENTATION OF THIS CLASS SHOULD BE THREAD-SAFE BECAUSE IT CAN BE INVOKED FROM PARALLEL THREADS SIMULTANEOUSLY
     * @throws ChannelException if some problems during subscribing to topic were occurred
     */
    void subscribe(RawMessageHandler onMessageHandler);

    /**
     * Unsubscribes from the associated topic. The handler (passed as argument to {@link #subscribe(RawMessageHandler)}) will no longer be invoked.
     * @throws ChannelException if some problems during unsubscribing to topic were occurred
     */
    void unsubscribe();

    /**
     * Callback interface for incoming message handler
     */
    interface RawMessageHandler {
        /**
         * Is called once a message arrives on the topic.
         *
         * @param jsonMessage incomming JSON message
         */
        void onMessage(String jsonMessage);
    }
}
