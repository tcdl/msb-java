package io.github.tcdl.msb.api;

import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.support.Utils;

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
     *
     * 0 means not to wait for responses at all.
     * -1 means to wait until {@link #responseTimeout} is reached.
     */
    private Integer waitForResponses;

    /**
     * Class to convert payload of response message to
     */
    private Class payloadClass;

    private MessageTemplate messageTemplate;

    private RequestOptions(Integer ackTimeout, Integer responseTimeout, Integer waitForResponses, Class payloadClass, MessageTemplate messageTemplate) {
        this.ackTimeout = ackTimeout;
        this.responseTimeout = responseTimeout;
        this.waitForResponses = waitForResponses;
        this.payloadClass = payloadClass;
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

    public Class getPayloadClass() {
        return payloadClass;
    }

    public MessageTemplate getMessageTemplate() {
        return messageTemplate;
    }

    @Override
    public String toString() {
        return "RequestOptions [ackTimeout=" + ackTimeout
                + ", responseTimeout=" + responseTimeout
                + ", waitForResponses=" + waitForResponses
                + ", payloadClass=" + payloadClass
                + (messageTemplate != null ? messageTemplate : "")
                + "]";
    }

    public static class Builder {

        private Integer ackTimeout;
        private Integer responseTimeout;
        private Integer waitForResponses;
        private Class payloadClass;
        private MessageTemplate messageTemplate;

        public Builder withAckTimeout(Integer ackTimeout) {
            this.ackTimeout = ackTimeout;
            return this;
        }

        public Builder withResponseTimeout(Integer responseTimeout) {
            this.responseTimeout = responseTimeout;
            return this;
        }

        public Builder withWaitForResponses(Integer waitForResponses) {
            this.waitForResponses = waitForResponses;
            return this;
        }

        public Builder withPayloadClass(Class payloadClass) {
            this.payloadClass = payloadClass;
            return this;
        }

        public Builder withMessageTemplate(MessageTemplate messageTemplate) {
            this.messageTemplate = messageTemplate;
            return this;
        }

        public RequestOptions build() {
            return new RequestOptions(ackTimeout, responseTimeout, waitForResponses, Utils.ifNull(payloadClass, Payload.class), messageTemplate);
        }
    }
}
