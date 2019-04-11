package io.github.tcdl.msb.api;

import org.apache.commons.lang3.Validate;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;

public class ActiveMQResponderOptions extends ResponderOptions {

    private final SubscriptionType subscriptionType;
    private final Set<String> bindingKeys;

    private ActiveMQResponderOptions(Set<String> bindingKeys,
                                   MessageTemplate messageTemplate,
                                   SubscriptionType subscriptionType) {
        super(bindingKeys, messageTemplate);
        this.bindingKeys = bindingKeys;
        this.subscriptionType = subscriptionType;
    }

    public SubscriptionType getSubscriptionType() {
        return subscriptionType;
    }

    public Set<String> getBindingKeys() {
        return bindingKeys;
    }

    public static class Builder extends ResponderOptions.Builder {

        private SubscriptionType subscriptionType;
        private Set<String> bindingKeys;

        public Builder withMessageTemplate(MessageTemplate responseMessageTemplate) {
            this.messageTemplate = responseMessageTemplate;
            return this;
        }

        public Builder withBindingKeys(Set<String> bindingKeys) {
            this.bindingKeys = bindingKeys;
            return this;
        }

        public Builder withSubscriptionType(@Nonnull SubscriptionType subscriptionType){
            Validate.notNull(subscriptionType);
            this.subscriptionType = subscriptionType;
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public ResponderOptions build() {
            Set<String> bindingKeys = this.bindingKeys == null || this.bindingKeys.isEmpty()
                    ? Collections.emptySet()
                    : this.bindingKeys;
            return new ActiveMQResponderOptions(bindingKeys, messageTemplate, subscriptionType);
        }
    }
}