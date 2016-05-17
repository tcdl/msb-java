package io.github.tcdl.msb.api;

import io.github.tcdl.msb.api.exception.ChannelException;
import io.github.tcdl.msb.api.exception.JsonConversionException;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * {@link Requester} enable user send message to bus and process responses for this messages if any expected.
 *
 * Expected number of messages and response timeouts is defined by {@link RequestOptions} during instance creation but can be updated by so called Acknowledgement
 * response mechanism in case we create Requester with {@literal RequestOptions.waitForResponses => 0} and received Acknowledgement response
 * before RequestOptions.ackTimeout or RequestOptions.responseTimeout (takes max of two).
 *
 * @param <T> expected payload type of response messages
 */
public interface Requester<T> {

    /**
     * Wraps a payload with protocol information and sends to bus.
     * In case Requester created with expectation for responses then process them.
     *
     * @param requestPayload payload which will be sent to bus
     * @throws ChannelException if an error is encountered during publishing to bus
     * @throws JsonConversionException if unable to parse message to JSON before sending to bus
     */
    void publish(Object requestPayload);

    /**
     * Wraps a payload with protocol information and sends to bus.
     * In case Requester created with expectation for responses then process them.
     *
     * @param requestPayload payload which will be sent to bus
     * @param tags to add to the message
     * @throws ChannelException if an error is encountered during publishing to bus
     * @throws JsonConversionException if unable to parse message to JSON before sending to bus
     */
    void publish(Object requestPayload, String... tags);

    /**
     * Wraps a payload with protocol information, preserves original message and sends to bus.
     * In case Requester created with expectation for responses then process them.
     *
     * @param requestPayload payload which will be sent to bus
     * @param originalMessage
     * @param tags to add to the message
     * @throws ChannelException if an error is encountered during publishing to bus
     * @throws JsonConversionException if unable to parse message to JSON before sending to bus
     */
    void publish(Object requestPayload, Message originalMessage, String... tags);

    /**
     * Wraps a payload with protocol information, preserves original message and sends to bus.
     * In case Requester created with expectation for responses then process them.
     *
     * @param requestPayload payload which will be sent to bus
     * @param originalMessage
     * @throws ChannelException if an error is encountered during publishing to bus
     * @throws JsonConversionException if unable to parse message to JSON before sending to bus
     */
    void publish(Object requestPayload, Message originalMessage);

    /**
     * Similar to {@link Requester#publish(java.lang.Object)} but expects exactly one response.
     * CompletableFuture response type adds a lot of flexibility to client implementation.
     * @return {@link CompletableFuture} that will be completed when first response is received.
     * CompletableFuture will be canceled if timeout occurs or acknowledge with different from 1 remaining responses
     * is received.
     */
    CompletableFuture<T> request(Object requestPayload);

     /**
     * Similar to {@link Requester#publish(java.lang.Object, java.lang.String...)} but expects exactly one response.
     * CompletableFuture response type adds a lot of flexibility to client implementation.
     * @return {@link CompletableFuture} that will be completed when first response is received.
     * CompletableFuture will be canceled if timeout occurs or acknowledge with different from 1 remaining responses
     * is received.
     */
    CompletableFuture<T> request(Object requestPayload, String... tags);

     /**
     * Similar to {@link Requester#publish(java.lang.Object, io.github.tcdl.msb.api.message.Message)}
     * but expects exactly one response. CompletableFuture response type adds a lot of flexibility to client implementation.
     * @return {@link CompletableFuture} that will be completed when first response is received.
     * CompletableFuture will be canceled if timeout occurs or acknowledge with different from 1 remaining responses
     * is received.
     */
    CompletableFuture<T> request(Object requestPayload, Message originalMessage);

    /**
     * Similar to
     * {@link io.github.tcdl.msb.api.Requester#publish(java.lang.Object, io.github.tcdl.msb.api.message.Message, java.lang.String...)}
     * but expects exactly one response. CompletableFuture response type adds a lot of flexibility to client implementation.
     * @return {@link CompletableFuture} that will be completed when first response is received.
     * CompletableFuture will be canceled if timeout occurs or acknowledge with different from 1 remaining responses
     * is received.
     */
    CompletableFuture<T> request(Object requestPayload, Message originalMessage, String... tags);

    /**
     * Registers a callback to be called when {@link Message} with {@link Acknowledge} part set is received.
     *
     * @param acknowledgeHandler callback to be called
     * @return requester
     */
    Requester<T> onAcknowledge(BiConsumer<Acknowledge, MessageContext> acknowledgeHandler);

    /**
     * Registers a callback to be called when response {@link Message} with payload part set of type {@literal T} is received.
     *
     * @param responseHandler callback to be called
     * @return requester
     * @throws JsonConversionException if unable to convert payload to type {@literal T}
     */
    Requester<T> onResponse(BiConsumer<T, MessageContext> responseHandler);

    /**
     * Registers a callback to be called when response {@link Message} with payload part set of is received.
     *
     * @param responseHandler callback to be called
     * @return requester
     */
    Requester<T> onRawResponse(BiConsumer<Message, MessageContext> responseHandler);
    
    /**
     * Registers a callback to be called when all expected responses for request message are processes or awaiting timeout for responses occurred.
     * Will be invoked only after all incoming responses will be processed.
     *
     * @param endHandler callback to be called
     * @return requester
     */
    Requester<T> onEnd(Callback<Void> endHandler);

    /**
     * Registers a callback to be called if an error is encountered during receiving a response from the bus
     * Will be invoked only after all incoming responses will be processed.
     *
     * @param errorHandler callback to be called
     * @return requester
     */
    Requester<T> onError(BiConsumer<Exception, Message> errorHandler);

}
