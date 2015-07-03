package io.github.tcdl.api;

import io.github.tcdl.RequesterImpl;

/**
 * Options used while constructing {@link RequesterImpl} that specify number and time to wait for acknowledgements or responses.
 * Not all combinations make sense for this object.
 * e.g: - setting {@literal waitForResponses <= 0} will not activate await for responses
 */
public class RequestOptions {

    private Integer ackTimeout;
    private Integer responseTimeout;
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
        if (waitForResponses == null || waitForResponses == -1) {
            return 0;
        }
        return waitForResponses;
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

    public static class Builder {

        private Integer ackTimeout;
        private Integer responseTimeout;
        private Integer waitForResponses;
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

        public Builder withMessageTemplate(MessageTemplate messageTemplate) {
            this.messageTemplate = messageTemplate;
            return this;
        }

        public RequestOptions build() {
            return new RequestOptions(ackTimeout, responseTimeout, waitForResponses, messageTemplate);
        }
    }
}
