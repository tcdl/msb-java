package io.github.tcdl.msb.api;

import joptsimple.internal.Strings;
import org.apache.commons.lang3.Validate;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;

/**
 * Specifies options for {@link ResponderServer}
 */
public class ResponderOptions {

    private final Set<String> bindingKeys;
    private final MessageTemplate messageTemplate;

    public static final ResponderOptions DEFAULTS = new Builder().build();

    protected ResponderOptions(Set<String> bindingKeys,
                               MessageTemplate messageTemplate) {

        this.bindingKeys = Collections.unmodifiableSet(bindingKeys);
        this.messageTemplate = messageTemplate;
    }

    @Nonnull
    public Set<String> getBindingKeys() {
        return bindingKeys;
    }

    public MessageTemplate getMessageTemplate() {
        return messageTemplate;
    }

    public static class Builder {

        protected Set<String> bindingKeys;
        protected MessageTemplate messageTemplate;

        /**
         * Each invocation REPLACES the old set of binding keys. Last one wins.
         */
        public Builder withBindingKeys(Set<String> bindingKeys) {
            this.bindingKeys = bindingKeys;
            return this;
        }

        public Builder withMessageTemplate(MessageTemplate responseMessageTemplate) {
            this.messageTemplate = responseMessageTemplate;
            return this;
        }

        public ResponderOptions build() {
            return new ResponderOptions(
                    bindingKeys == null ? Collections.singleton(Strings.EMPTY) : bindingKeys,
                    messageTemplate == null ? new MessageTemplate() : messageTemplate);
        }
    }
}
