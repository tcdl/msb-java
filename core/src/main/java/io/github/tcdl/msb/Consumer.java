package io.github.tcdl.msb;

import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.MetaMessage;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.monitor.agent.ChannelMonitorAgent;
import io.github.tcdl.msb.support.JsonValidator;
import io.github.tcdl.msb.support.Utils;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * {@link Consumer} is a component responsible for consuming messages from the bus.
 */
public class Consumer {

    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    private final ConsumerAdapter rawAdapter;
    private final String topic;
    private MsbConfig msbConfig;
    private ChannelMonitorAgent channelMonitorAgent;
    private Clock clock;
    private MessageHandler messageHandler;
    private JsonValidator validator;
    private ObjectMapper messageMapper;

    /**
     * @param rawAdapter instance of {@link ConsumerAdapter} that allows to receive messages from message bus
     * @param topic
     * @param messageHandler interface that user can implement to handle received message
     * @param msbConfig consumer configs
     * @param clock
     * @param channelMonitorAgent
     * @param validator validates incoming messages
     * @param messageMapper message deserializer
     */
    public Consumer(ConsumerAdapter rawAdapter, String topic, MessageHandler messageHandler, MsbConfig msbConfig,
            Clock clock, ChannelMonitorAgent channelMonitorAgent, JsonValidator validator, ObjectMapper messageMapper) {

        LOG.debug("Creating consumer for topic: {}", topic);
        Validate.notNull(rawAdapter, "the 'rawAdapter' must not be null");
        Validate.notNull(topic, "the 'topic' must not be null");
        Validate.notNull(messageHandler, "the 'messageHandler' must not be null");
        Validate.notNull(msbConfig, "the 'msbConfig' must not be null");
        Validate.notNull(clock, "the 'clock' must not be null");
        Validate.notNull(channelMonitorAgent, "the 'channelMonitorAgent' must not be null");
        Validate.notNull(validator, "the 'validator' must not be null");
        Validate.notNull(messageMapper, "the 'messageMapper' must not be null");

        this.rawAdapter = rawAdapter;
        this.topic = topic;
        this.messageHandler = messageHandler;
        this.msbConfig = msbConfig;
        this.clock = clock;
        this.channelMonitorAgent = channelMonitorAgent;
        this.validator = validator;
        this.messageMapper = messageMapper;

        this.rawAdapter.subscribe(this::handleRawMessage);
    }

    /**
     * Stop consuming messages for specified topic.
     */
    public void end() {
        LOG.debug("Shutting down consumer for topic {}", topic);
        rawAdapter.unsubscribe();
    }

    /**
     * Process incoming message.
     *
     * @param jsonMessage message to process
     */
    protected void handleRawMessage(String jsonMessage, AcknowledgementHandlerInternal acknowledgeHandler) {
        LOG.debug("Topic [{}] message received [{}]", this.topic, jsonMessage);
        channelMonitorAgent.consumerMessageReceived(topic);

        Message message;

        try {
            message = parseMessage(jsonMessage);
        } catch (Exception e) {
            LOG.error("Unable to process consumed message {}", jsonMessage, e);
            acknowledgeHandler.autoReject();
            return;
        }

        if (isMessageExpired(message)) {
            LOG.warn("Expired message: {}", jsonMessage);
            acknowledgeHandler.autoReject();
            return;
        }

        try {
            messageHandler.handleMessage(message, acknowledgeHandler);
        } catch (Exception e) {
            LOG.warn("Error while trying to handle a message: {}", jsonMessage, e);
            acknowledgeHandler.autoRetry();
        }
    }

    private Message parseMessage(String jsonMessage) {
        if (msbConfig.getSchema() != null && !Utils.isServiceTopic(topic) && msbConfig.isValidateMessage()) {
            LOG.debug("Validating schema for {}", jsonMessage);
            validator.validate(jsonMessage, msbConfig.getSchema());
        }
        LOG.debug("Parsing message {}", jsonMessage);
        Message result = Utils.fromJson(jsonMessage, Message.class, messageMapper);
        LOG.debug("Message has been successfully parsed {}", jsonMessage);
        return result;
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