package io.github.tcdl;

import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.exception.JsonConversionException;
import io.github.tcdl.exception.JsonSchemaValidationException;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.MetaMessage;
import io.github.tcdl.monitor.ChannelMonitorAgent;
import io.github.tcdl.support.Utils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rdro on 4/23/2015.
 */
public class Consumer {

    public static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    private final Adapter rawAdapter;
    private final String topic;
    private MsbConfigurations msbConfig;
    private ChannelMonitorAgent channelMonitorAgent;
    private Clock clock;

    private List<Subscriber> subscribers;

    public Consumer(Adapter rawAdapter, String topic, MsbConfigurations msbConfig, Clock clock, ChannelMonitorAgent channelMonitorAgent) {
        LOG.debug("Creating consumer for topic: {}", topic);
        Validate.notNull(rawAdapter, "the 'rawAdapter' must not be null");
        Validate.notNull(topic, "the 'topic' must not be null");
        Validate.notNull(msbConfig, "the 'msbConfig' must not be null");
        Validate.notNull(clock, "the 'clock' must not be null");
        Validate.notNull(channelMonitorAgent, "the 'channelMonitorAgent' must not be null");

        this.rawAdapter = rawAdapter;
        this.topic = topic;
        this.msbConfig = msbConfig;
        this.clock = clock;
        this.channelMonitorAgent = channelMonitorAgent;
        this.subscribers = new ArrayList<>();
    }

    public Consumer subscribe(Callback<Message> messageHandler) {
        subscribe(messageHandler, null);
        return this;
    }

    public synchronized Consumer subscribe(Callback<Message> messageHandler, Callback<Exception> errorHandler) {
        Validate.notNull(messageHandler, "the 'messageHandler' must not be null");

        subscribers.add(new Subscriber(messageHandler, errorHandler));
        rawAdapter.subscribe(this::handleRawMessage);

        return this;
    }

    public void end() {
        LOG.debug("Shutting down consumer for topic {}", topic);
        rawAdapter.unsubscribe();
    }

    protected void handleRawMessage(String jsonMessage) {
        LOG.debug("Topic [{}] message received [{}]", this.topic, jsonMessage);
        Exception error = null;
        Message message = null;

        try {
            if (msbConfig.getSchema() != null
                    && !Utils.isServiceTopic(topic)) {
                Utils.validateJsonWithSchema(jsonMessage,
                        msbConfig.getSchema());
            }
            message = Utils.fromJson(jsonMessage,
                    Message.class);
        } catch (JsonConversionException | JsonSchemaValidationException e) {
            error = e;
        }

        if (channelMonitorAgent != null) {
            channelMonitorAgent.consumerMessageReceived(topic);
        }

        synchronized (this) {
            for (Subscriber subscriber : subscribers) {

                if (error != null) {
                    Callback<Exception> errorHandler = subscriber.getErrorHandler();
                    if (errorHandler != null) {
                        errorHandler.call(error);
                    }
                } else {
                    if (isMessageExpired(message)) {
                        return;
                    }

                    Callback<Message> messageHandler = subscriber.getMessageHandler();
                    if (messageHandler != null) {
                        messageHandler.call(message);
                    }
                }
            }
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

    private static class Subscriber {

        private Callback<Message> messageHandler;
        private Callback<Exception> errorHandler;

        public Subscriber(Callback<Message> messageHandler, Callback<Exception> errorHandler) {
            this.messageHandler = messageHandler;
            this.errorHandler = errorHandler;
        }

        public Callback<Message> getMessageHandler() {
            return messageHandler;
        }

        public Callback<Exception> getErrorHandler() {
            return errorHandler;
        }
    }
}
