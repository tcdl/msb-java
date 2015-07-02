package io.github.tcdl.api;

import io.github.tcdl.api.message.Message;
import io.github.tcdl.api.message.payload.Payload;

/**
 * Responsible for creating responses and acknowledgements and sending them to the bus.
 */
public interface Responder {

    /**
     * Send acknowledge message.
     *
     * @param timeoutMs time to wait for remaining responses
     * @param responsesRemaining expected number of responses
     * @return acknowledgement message which was sent
     */
    Message sendAck(Integer timeoutMs, Integer responsesRemaining);

    /**
     * Send payload message.
     *
     * @param responsePayload payload which will be used to create response message
     * @return response message which was sent
     */
    Message send(Payload responsePayload);
}