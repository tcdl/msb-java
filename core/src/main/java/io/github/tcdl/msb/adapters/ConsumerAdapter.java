package io.github.tcdl.msb.adapters;

import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.api.exception.ChannelException;

/**
 * {@link ConsumerAdapter} allows to receive messages from message bus. One adapter instance is associated with specific topic.
 *
 * Implementations should allow to connect to and use different kinds of buses (like AMQP, JMS, etc)
 */
public interface ConsumerAdapter {
    /**
     * Subscribes the given message handler to the associated topic
     * @param onMessageHandler this handler is invoked once message arrives on the topic.
     *                         SHOULD BE THREAD-SAFE BECAUSE IT CAN BE INVOKED FROM PARALLEL THREADS SIMULTANEOUSLY
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
         * @param acknowledgementHandler confirm/reject message handler
         */
        void onMessage(String jsonMessage, AcknowledgementHandlerInternal acknowledgementHandler);
    }
    
}
