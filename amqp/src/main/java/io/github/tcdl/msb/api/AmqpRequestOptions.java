package io.github.tcdl.msb.api;

import org.apache.commons.lang3.Validate;

import javax.annotation.Nonnull;


public class AmqpRequestOptions extends RequestOptions {

    private final ExchangeType exchangeType;

    private AmqpRequestOptions(Integer ackTimeout,
                               Integer responseTimeout,
                               Integer waitForResponses,
                               MessageTemplate messageTemplate,
                               String forwardNamespace,
                               String routingKey,
                               ExchangeType exchangeType) {

        super(ackTimeout, responseTimeout, waitForResponses, messageTemplate, forwardNamespace, routingKey);
        this.exchangeType = exchangeType;
    }

    public ExchangeType getExchangeType() {
        return exchangeType;
    }

    @Override
    public RequestOptions.Builder asBuilder() {
        return ((AmqpRequestOptions.Builder) (new Builder().from(this))).withExchangeType(this.exchangeType);
    }

    public static class Builder extends RequestOptions.Builder {

        private ExchangeType exchangeType;

        public Builder withExchangeType(ExchangeType exchangeType){
            this.exchangeType = exchangeType;
            return this;
        }

        @Override
        public RequestOptions build() {
            return new AmqpRequestOptions(ackTimeout, responseTimeout, waitForResponses, messageTemplate,
                    forwardNamespace, routingKey, exchangeType);
        }
    }
}
