package io.github.tcdl.api;

import io.github.tcdl.api.exception.ChannelException;
import io.github.tcdl.api.exception.JsonConversionException;
import io.github.tcdl.api.message.Acknowledge;
import io.github.tcdl.api.message.Message;
import io.github.tcdl.api.message.payload.Payload;

import java.util.List;

/**
 * {@link Requester} enable user send message to bus and process responses for this messages if any expected.
 *
 * Expected responses are matched by correlationId from original request.
 *
 * Expected number of messages and response timeouts is defined by {@link RequestOptions} during instance creation but can be updated by so called Acknowledgement
 * response mechanism in case we create Requester with {@literal RequestOptions.waitForResponses => 0} and received Acknowledgement response
 * before RequestOptions.ackTimeout or RequestOptions.responseTimeout (takes max of two).
 *
 * Please note: RequestOptions.waitForResponses represent number of response messages  with {@link Payload} set and in case we received
 * all expected before RequestOptions.responseTimeout we don't wait for Acknowledgement response and RequestOptions.ackTimeout is not used.
 */
public interface Requester {

    /**
     * Wraps a payload with protocol information and sends to bus.
     * In case Requester created with expectation for responses then process them.
     *
     * @param requestPayload payload which will be sent to bus
     * @throws ChannelException if an error is encountered during publishing to bus
     * @throws JsonConversionException if unable to parse message to JSON before sending to bus
     */
    void publish(Payload requestPayload);
    
    /**
     * Registers a callback to be called when {@link Message} with {@link Acknowledge} property set is received.
     *
     * @param acknowledgeHandler callback to be called
     * @return requester
     */
    Requester onAcknowledge(Callback<Acknowledge> acknowledgeHandler);

    /**
     * Registers a callback to be called when response {@link Message} with {@link Payload} property set is received.
     *
     * @param responseHandler callback to be called
     * @return requester
     */
    Requester onResponse(Callback<Payload> responseHandler);
    
    /**
     * Registers a callback to be called when all expected responses for request message are processes or awaiting timeout for responses occurred.
     *
     * @param endHandler callback to be called
     * @return requester
     */
    Requester onEnd(Callback<List<Message>> endHandler);
}
