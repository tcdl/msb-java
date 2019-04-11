package io.github.tcdl.msb.api;

public class ActiveMQRequestOptions extends RequestOptions {

    private final SubscriptionType subscriptionType;

    private ActiveMQRequestOptions(Integer ackTimeout,
                               Integer responseTimeout,
                               Integer waitForResponses,
                               MessageTemplate messageTemplate,
                               String forwardNamespace,
                               String messageSelector,
                               SubscriptionType subscriptionType) {

        super(ackTimeout, responseTimeout, waitForResponses, messageTemplate, forwardNamespace, messageSelector);
        this.subscriptionType = subscriptionType;
    }

    public SubscriptionType getSubscriptionType() {
        return subscriptionType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RequestOptions.Builder asBuilder() {
        return ((ActiveMQRequestOptions.Builder) (new Builder().from(this))).withSubscriptionType(this.subscriptionType);
    }

    public static class Builder extends RequestOptions.Builder {

        private SubscriptionType subscriptionType;

        public Builder withSubscriptionType(SubscriptionType subscriptionType){
            this.subscriptionType = subscriptionType;
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public RequestOptions build() {
            return new ActiveMQRequestOptions(ackTimeout, responseTimeout, waitForResponses, messageTemplate,
                    forwardNamespace, routingKey, subscriptionType);
        }
    }
}