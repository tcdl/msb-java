package io.github.tcdl.msb.acknowledge;

/**
 * Adapter that provides low-level acknowledgement management methods for {@link AcknowledgementHandlerImpl}.
 */
public interface AcknowledgementAdapter {

    /**
     * Confirm a message.
     * @throws Exception
     */
    void confirm() throws Exception;

    /**
     * Reject a message.
     * @throws Exception
     */
    void reject() throws Exception;

    /**
     * Requeue a message.
     * @throws Exception
     */
    void retry() throws Exception;
}
