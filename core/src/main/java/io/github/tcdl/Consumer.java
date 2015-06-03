package io.github.tcdl;

import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.events.Event;
import io.github.tcdl.events.EventEmitter;
import io.github.tcdl.events.SingleArgEventHandler;
import io.github.tcdl.events.TwoArgsEventHandler;
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

import static io.github.tcdl.events.Event.ERROR_EVENT;
import static io.github.tcdl.events.Event.MESSAGE_EVENT;

/**
 * Created by rdro on 4/23/2015.
 */
public class Consumer {

    public static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    private EventEmitter eventEmitter;
    private final Adapter rawAdapter;
    private final String topic;
    private MsbConfigurations msbConfig;
    private ChannelMonitorAgent channelMonitorAgent;
    private Clock clock;

    public Consumer(Adapter rawAdapter, String topic, EventEmitter eventEmitter, MsbConfigurations msbConfig, Clock clock, ChannelMonitorAgent channelMonitorAgent) {
        LOG.debug("Creating consumer for topic: {}", topic);
        Validate.notNull(rawAdapter, "the 'rawAdapter' must not be null");
        Validate.notNull(topic, "the 'topic' must not be null");
        Validate.notNull(eventEmitter, "the 'eventEmitter' must not be null");
        Validate.notNull(msbConfig, "the 'msbConfig' must not be null");
        Validate.notNull(clock, "the 'clock' must not be null");
        Validate.notNull(channelMonitorAgent, "the 'channelMonitorAgent' must not be null");

        this.rawAdapter = rawAdapter;
        this.topic = topic;
        this.eventEmitter = eventEmitter;
        this.msbConfig = msbConfig;
        this.clock = clock;
        this.channelMonitorAgent = channelMonitorAgent;
    }

    public Consumer subscribe() {
        // merge msgOptions with msbConfig
        // do other stuff
        rawAdapter.subscribe(this::handleRawMessage);

        return this;
    }

    public void end() {
        LOG.debug("Shutting down consumer for topic {}", topic);
        rawAdapter.unsubscribe();
    }

    public <A1> Consumer on(Event event, SingleArgEventHandler<A1> eventHandler) {
        eventEmitter.on(event, eventHandler);
        return this;
    }

    public <A1, A2> Consumer on(Event event, TwoArgsEventHandler<A1, A2> eventHandler) {
        eventEmitter.on(event, eventHandler);
        return this;
    }

    public <A1> Consumer emit(Event event, A1 arg) {
        eventEmitter.emit(event, arg);
        return this;
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

        if (error != null) {
            eventEmitter.emit(ERROR_EVENT, error);
            return;
        }

        if (isMessageExpired(message))
            return;

        if (channelMonitorAgent != null) {
            channelMonitorAgent.consumerMessageReceived(topic);
        }

        eventEmitter.emit(MESSAGE_EVENT, message);
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
