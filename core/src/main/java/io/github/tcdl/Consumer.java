package io.github.tcdl;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import io.github.tcdl.adapters.ConsumerAdapter;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.exception.JsonConversionException;
import io.github.tcdl.exception.JsonSchemaValidationException;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.MetaMessage;
import io.github.tcdl.monitor.ChannelMonitorAgent;
import io.github.tcdl.support.JsonValidator;
import io.github.tcdl.support.Utils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Consumer} is a component responsible for consuming messages from the bus.
 *
 * Created by rdro on 4/23/2015.
 */
public class Consumer {

    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    private final ConsumerAdapter rawAdapter;
    private final String topic;
    private MsbConfigurations msbConfig;
    private ChannelMonitorAgent channelMonitorAgent;
    private Clock clock;

    private Subscriber messageHandler;
    private JsonValidator validator;

    public Consumer(ConsumerAdapter rawAdapter, String topic, MsbConfigurations msbConfig,
            Clock clock, ChannelMonitorAgent channelMonitorAgent, JsonValidator validator) {

        LOG.debug("Creating consumer for topic: {}", topic);
        Validate.notNull(rawAdapter, "the 'rawAdapter' must not be null");
        Validate.notNull(topic, "the 'topic' must not be null");
        Validate.notNull(msbConfig, "the 'msbConfig' must not be null");
        Validate.notNull(clock, "the 'clock' must not be null");
        Validate.notNull(channelMonitorAgent, "the 'channelMonitorAgent' must not be null");
        Validate.notNull(validator, "the 'validator' must not be null");

        this.rawAdapter = rawAdapter;
        this.rawAdapter.subscribe(this::handleRawMessage);
        this.topic = topic;
        this.msbConfig = msbConfig;
        this.clock = clock;
        this.channelMonitorAgent = channelMonitorAgent;
        this.validator = validator;
    }

    /**
     * Adds a handler that invoked when message being received.
     *
     * @param subscriberMessageHandler
     */
    public void subscribe(Subscriber subscriberMessageHandler) {
        Validate.notNull(subscriberMessageHandler, "the 'subscriberMessageHandler' must not be null");
        messageHandler = subscriberMessageHandler;
    }

    /**
     * Stop consuming messages for specified topic
     */
    public void end() {
        LOG.debug("Shutting down consumer for topic {}", topic);
        rawAdapter.unsubscribe();
    }

    protected void handleRawMessage(String jsonMessage) {
        LOG.debug("Topic [{}] message received [{}]", this.topic, jsonMessage);
        channelMonitorAgent.consumerMessageReceived(topic);

        try {
            if (msbConfig.getSchema() != null && !Utils.isServiceTopic(topic) && msbConfig.isValidateMessage()) {
                LOG.debug("Validating schema for {}", jsonMessage);
                validator.validate(jsonMessage, msbConfig.getSchema());
            }
            LOG.debug("Parsing message {}", jsonMessage);
            Message message = Utils.fromJson(jsonMessage, Message.class);
            LOG.debug("Message has been successfully parsed {}", jsonMessage);

            if (!isMessageExpired(message)) {
                messageHandler.handleMessage(message);
            } else {
                LOG.warn("Expired message: {}", jsonMessage);
            }
        } catch (JsonConversionException | JsonSchemaValidationException e) {
            LOG.error("Got error while parsing message {}", jsonMessage, e);
        }
    }

    private boolean isMessageExpired(Message message) {
        MetaMessage meta = message.getMeta();
        if (meta == null || meta.getTtl() == null) {
            return false;
        }

        Integer ttl = meta.getTtl();
        Instant expiryTime = meta.getCreatedAt().plus(ttl, ChronoUnit.MILLIS);
        Instant now = clock.instant();

        return expiryTime.isBefore(now);
    }
}