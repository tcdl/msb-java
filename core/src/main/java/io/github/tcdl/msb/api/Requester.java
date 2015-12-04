package io.github.tcdl.msb.api;

import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.api.exception.ChannelException;
import io.github.tcdl.msb.api.exception.JsonConversionException;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;

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
     * @param tag to add to the message
     * @throws ChannelException if an error is encountered during publishing to bus
     * @throws JsonConversionException if unable to parse message to JSON before sending to bus
     */
    void publish(Object requestPayload, String tag);

    /**
     * Wraps a payload with protocol information, preserves original message and sends to bus.
     * In case Requester created with expectation for responses then process them.
     *
     * @param requestPayload payload which will be sent to bus
     * @param originalMessage
     * @param tag to add to the message
     * @throws ChannelException if an error is encountered during publishing to bus
     * @throws JsonConversionException if unable to parse message to JSON before sending to bus
     */
    void publish(Object requestPayload, Message originalMessage, String tag);

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
     * Registers a callback to be called when {@link Message} with {@link Acknowledge} part set is received.
     *
     * @param acknowledgeHandler callback to be called
     * @return requester
     */
    Requester<T> onAcknowledge(BiConsumer<Acknowledge, ConsumerAdapter.AcknowledgementHandler> acknowledgeHandler);

    /**
     * Registers a callback to be called when response {@link Message} with payload part set of type {@literal T} is received.
     *
     * @param responseHandler callback to be called
     * @return requester
     * @throws JsonConversionException if unable to convert payload to type {@literal T}
     */
    Requester<T> onResponse(BiConsumer<T, ConsumerAdapter.AcknowledgementHandler> responseHandler);

    /**
     * Registers a callback to be called when response {@link Message} with payload part set of is received.
     *
     * @param responseHandler callback to be called
     * @return requester
     */
    Requester<T> onRawResponse(BiConsumer<Message, ConsumerAdapter.AcknowledgementHandler> responseHandler);
    
    /**
     * Registers a callback to be called when all expected responses for request message are processes or awaiting timeout for responses occurred.
     *
     * @param endHandler callback to be called
     * @return requester
     */
    Requester<T> onEnd(Callback<Void> endHandler);
}
