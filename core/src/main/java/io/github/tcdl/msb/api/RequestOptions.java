package io.github.tcdl.msb.api;

import org.apache.commons.lang3.StringUtils;

/**
 * Specifies waiting policy (for acknowledgements and responses) for requests sent using {@link Requester}.
 */
public class RequestOptions {

    public static final int WAIT_FOR_RESPONSES_UNTIL_TIMEOUT = -1;

    public static final RequestOptions DEFAULTS = new Builder().build();
    /**
     * Max time (in milliseconds) to wait for acknowledgements.
     */
    private final Integer ackTimeout;

    /**
     * Max time (in milliseconds) to wait for responses and acknowledgements. Once this timeout is reached we stop waiting for responses even if
     * {@link #waitForResponses} has not been reached. Beware that acks may adjust this timeout.
     */
    private final Integer responseTimeout;

    /**
     * Number of responses to wait for. Once this number is reached (and {@link #ackTimeout} passed) we stop waiting for responses even if
     * {@link #responseTimeout} has not been reached. Beware that acks may adjust this number.
     * <p/>
     * 0 means not to wait for responses at all.
     * -1 means to wait until {@link #responseTimeout} is reached.
     */
    private final Integer waitForResponses;

    /**
     * A namespace for messages forwarding performed by a consumer.
     */
    private final String forwardNamespace;

    private final MessageTemplate messageTemplate;

    private final String routingKey;

    protected RequestOptions(Integer ackTimeout, Integer responseTimeout, Integer waitForResponses, MessageTemplate messageTemplate, String forwardNamespace, String routingKey) {
        this.ackTimeout = ackTimeout;
        this.responseTimeout = responseTimeout;
        this.waitForResponses = waitForResponses;
        this.messageTemplate = messageTemplate;
        this.forwardNamespace = forwardNamespace;
        this.routingKey = routingKey;
    }

    public Integer getAckTimeout() {
        return ackTimeout;
    }

    public Integer getResponseTimeout() {
        return responseTimeout;
    }

    public int getWaitForResponses() {
        if (waitForResponses == null || waitForResponses == -1) {
            // use for infinite number or expected responses
            return WAIT_FOR_RESPONSES_UNTIL_TIMEOUT;
        } else {
            return waitForResponses;
        }
    }

    public MessageTemplate getMessageTemplate() {
        return messageTemplate;
    }

    public String getForwardNamespace() {
        return forwardNamespace;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public Builder asBuilder() {
        return new RequestOptions.Builder().from(this);
    }

    @Override
    public String toString() {
        return "RequestOptions [ackTimeout=" + ackTimeout
                + ", responseTimeout=" + responseTimeout
                + ", waitForResponses=" + waitForResponses
                + ", forwardNamespace=" + forwardNamespace
                + (messageTemplate != null ? messageTemplate : "")
                + "]";
    }

    public static class Builder {

        protected String routingKey;
        protected Integer ackTimeout;
        protected Integer responseTimeout;
        protected Integer waitForResponses;
        protected MessageTemplate messageTemplate;
        protected String forwardNamespace;

        public Builder withRoutingKey(String routingKey) {
            this.routingKey = routingKey;
            return this;
        }

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

        public Builder withForwardNamespace(String forward) {
            this.forwardNamespace = forward;
            return this;
        }

        /**
         * Convenience method to prepare Builder with properties equal to {@literal source} properties.
         * Is useful for cases when almost same RequestOptions except one or two properties are needed.
         */
        protected Builder from(RequestOptions source) {
            this.ackTimeout = source.ackTimeout;
            this.responseTimeout = source.responseTimeout;
            this.waitForResponses = source.waitForResponses;
            this.messageTemplate = source.messageTemplate;
            this.forwardNamespace = source.forwardNamespace;
            this.routingKey = source.routingKey;
            return this;
        }

        public RequestOptions build() {
            return new RequestOptions(ackTimeout, responseTimeout, waitForResponses, messageTemplate, forwardNamespace,
                    routingKey != null ? routingKey : StringUtils.EMPTY);
        }
    }
}
