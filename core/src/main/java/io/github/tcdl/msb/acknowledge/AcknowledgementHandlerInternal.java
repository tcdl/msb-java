package io.github.tcdl.msb.acknowledge;

import io.github.tcdl.msb.api.AcknowledgementHandler;

/**
 * Interface for message acknowledgement used internally for implicit messages acknowledge.
 */
public interface AcknowledgementHandlerInternal extends AcknowledgementHandler {

    /**
     * Implicit message acknowledge request invoked after client callback execution.
     */
    void autoConfirm();

    /**
     * Implicit message reject request invoked when a message is expired or corrupted.
     */
    void autoReject();

    /**
     * Implicit message requeue request invoked when there was an exception during a client callback execution.
     */
    void autoRetry();
}
