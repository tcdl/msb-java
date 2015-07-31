package io.github.tcdl.msb.api;

import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.Payload;

/**
 * Responsible for creating responses and acknowledgements and sending them to the bus.
 */
public interface Responder {

    /**
     * Send acknowledge message.
     *
     * @param timeoutMs time to wait for remaining responses
     * @param responsesRemaining expected number of responses
     */
    void sendAck(Integer timeoutMs, Integer responsesRemaining);

    /**
     * Send payload message.
     *
     * @param responsePayload payload which will be used to create response message
     */
    void send(Payload<?, ?, ?, ?> responsePayload);

    /**
     * @return original message to send a response to
     */
    Message getOriginalMessage();
}