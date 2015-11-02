package io.github.tcdl.msb.api;

import io.github.tcdl.msb.api.message.payload.RestPayload;

/**
 * Specifies waiting policy (for acknowledgements and responses) for requests sent using {@link Requester}.
 */
public class RequestOptions {

    /**
     * Min time (in milliseconds) to wait for acknowledgements.
     */
    private Integer ackTimeout;

    /**
     * Max time (in milliseconds) to wait for responses and acknowledgements. Once this timeout is reached we stop waiting for responses even if
     * {@link #waitForResponses} has not been reached. Beware that acks may adjust this timeout.
     */
    private Integer responseTimeout;

    /**
     * Number of responses to wait for. Once this number is reached (and {@link #ackTimeout} passed) we stop waiting for responses even if
     * {@link #responseTimeout} has not been reached. Beware that acks may adjust this number.
     * <p/>
     * 0 means not to wait for responses at all.
     * -1 means to wait until {@link #responseTimeout} is reached.
     */
    private Integer waitForResponses;

    private MessageTemplate messageTemplate;

    private RequestOptions(Integer ackTimeout, Integer responseTimeout, Integer waitForResponses, MessageTemplate messageTemplate) {
        this.ackTimeout = ackTimeout;
        this.responseTimeout = responseTimeout;
        this.waitForResponses = waitForResponses;
        this.messageTemplate = messageTemplate;
    }

    public Integer getAckTimeout() {
        return ackTimeout;
    }

    public Integer getResponseTimeout() {
        return responseTimeout;
    }

    public Integer getWaitForResponses() {
        if (waitForResponses == null) {
            return 0;
        } else {
            return waitForResponses;
        }
    }

    public boolean isWaitForResponses() {
        return getWaitForResponses() != 0;
    }

    public MessageTemplate getMessageTemplate() {
        return messageTemplate;
    }

    @Override
    public String toString() {
        return "RequestOptions [ackTimeout=" + ackTimeout
                + ", responseTimeout=" + responseTimeout
                + ", waitForResponses=" + waitForResponses
                + (messageTemplate != null ? messageTemplate : "")
                + "]";
    }

    public static class Builder<T extends RestPayload> {

        private Integer ackTimeout;
        private Integer responseTimeout;
        private Integer waitForResponses;
        private MessageTemplate messageTemplate;

        public Builder<T> withAckTimeout(Integer ackTimeout) {
            this.ackTimeout = ackTimeout;
            return this;
        }

        public Builder<T> withResponseTimeout(Integer responseTimeout) {
            this.responseTimeout = responseTimeout;
            return this;
        }

        public Builder<T> withWaitForResponses(Integer waitForResponses) {
            this.waitForResponses = waitForResponses;
            return this;
        }

        public Builder<T> withMessageTemplate(MessageTemplate messageTemplate) {
            this.messageTemplate = messageTemplate;
            return this;
        }

        public RequestOptions build() {
            return new RequestOptions(ackTimeout, responseTimeout, waitForResponses, messageTemplate);
        }
    }
}
