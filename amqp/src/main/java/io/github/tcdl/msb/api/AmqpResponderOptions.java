package io.github.tcdl.msb.api;

import org.apache.commons.lang3.Validate;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;


public class AmqpResponderOptions extends ResponderOptions{

    public static final String MATCH_ALL_BINDING_KEY = "#";
    private final ExchangeType exchangeType;

    protected AmqpResponderOptions(Set<String> bindingKeys,
                                   MessageTemplate messageTemplate,
                                   ExchangeType exchangeType) {
        super(bindingKeys, messageTemplate);
        this.exchangeType = exchangeType;
    }

    public ExchangeType getExchangeType() {
        return exchangeType;
    }

    public static class Builder extends ResponderOptions.Builder {

        private ExchangeType exchangeType;

        public Builder withMessageTemplate(MessageTemplate responseMessageTemplate) {
            this.messageTemplate = responseMessageTemplate;
            return this;
        }

        public Builder withBindingKeys(Set<String> bindingKeys) {
            this.bindingKeys = bindingKeys;
            return this;
        }

        public Builder withExchangeType(@Nonnull ExchangeType exchangeType){
            Validate.notNull(exchangeType);
            this.exchangeType = exchangeType;
            return this;
        }

        @Override
        public ResponderOptions build() {
            Set<String> bindingKeys = this.bindingKeys == null || this.bindingKeys.isEmpty()
                    ? Collections.singleton(MATCH_ALL_BINDING_KEY)
                    : this.bindingKeys;

            return new AmqpResponderOptions(bindingKeys, messageTemplate, exchangeType);
        }
    }
}
