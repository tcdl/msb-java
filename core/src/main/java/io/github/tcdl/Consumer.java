package io.github.tcdl;

import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.events.TwoArgsEventHandler;
import io.github.tcdl.exception.JsonConversionException;
import io.github.tcdl.exception.JsonSchemaValidationException;
import io.github.tcdl.messages.Message;
import io.github.tcdl.support.Utils;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rdro on 4/23/2015.
 */
public class Consumer {

    public static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    private final Adapter rawAdapter;
    private final String topic;
    private final TwoArgsEventHandler<Message, Exception> messageHandler;
    private final MsbConfigurations msbConfig; // not sure we need this here    

    public Consumer(Adapter rawAdapter, String topic, TwoArgsEventHandler<Message, Exception> messageHandler, MsbConfigurations msbConfig) {
        LOG.debug("Creating consumer for topic: {}", topic);
        Validate.notNull(rawAdapter, "the 'rawAdapter' must not be null");
        Validate.notNull(topic, "the 'topic' must not be null");
        Validate.notNull(messageHandler, "the 'messageHandler' must not be null");
        Validate.notNull(msbConfig, "the 'msbConfig' must not be null");

        this.rawAdapter = rawAdapter;
        this.topic = topic;
        this.messageHandler = messageHandler;
        this.msbConfig = msbConfig;
    }

    public Consumer subscribe() {
        // merge msgOptions with msbConfig
        // do other stuff
        rawAdapter.subscribe((jsonMessage) -> {
            LOG.debug("Topic [{}] message received [{}]", this.topic, jsonMessage);
            Exception error = null;
            Message message = null;

            try {
                if (msbConfig.getSchema() != null
                        && !isServiceChannel(topic)) {
                    Utils.validateJsonWithSchema(jsonMessage,
                            msbConfig.getSchema());
                }
                message = Utils.fromJson(jsonMessage,
                        Message.class);
            } catch (JsonConversionException | JsonSchemaValidationException e) {
                error = e;
            }

            this.messageHandler.onEvent(message, error);

        });

        return this;
    }

    public void end() {
        LOG.debug("Shutting down consumer for topic {}", topic);
        rawAdapter.unsubscribe();
    }

    private boolean isServiceChannel(String topic) {
        return topic.charAt(0) == '_';
    }
}
