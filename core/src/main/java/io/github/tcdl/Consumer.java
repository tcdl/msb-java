package io.github.tcdl;

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

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by rdro on 4/23/2015.
 */
public class Consumer {

    public static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    private final ConsumerAdapter rawAdapter;
    private final String topic;
    private MsbConfigurations msbConfig;
    private ChannelMonitorAgent channelMonitorAgent;
    private Clock clock;

    private ConcurrentLinkedQueue<Subscriber> subscribers;
    private JsonValidator jsonValidator;

    public Consumer(ConsumerAdapter rawAdapter, String topic, MsbConfigurations msbConfig, Clock clock, ChannelMonitorAgent channelMonitorAgent) {
        LOG.debug("Creating consumer for topic: {}", topic);
        Validate.notNull(rawAdapter, "the 'rawAdapter' must not be null");
        Validate.notNull(topic, "the 'topic' must not be null");
        Validate.notNull(msbConfig, "the 'msbConfig' must not be null");
        Validate.notNull(clock, "the 'clock' must not be null");
        Validate.notNull(channelMonitorAgent, "the 'channelMonitorAgent' must not be null");

        this.rawAdapter = rawAdapter;
        this.rawAdapter.subscribe(this::handleRawMessage);
        this.topic = topic;
        this.msbConfig = msbConfig;
        this.clock = clock;
        this.channelMonitorAgent = channelMonitorAgent;
        this.subscribers = new ConcurrentLinkedQueue<>();
        this.jsonValidator = new JsonValidator();
    }

    /**
     * Adds a subscriber to call it when message received
     */
    public void subscribe(Subscriber subscriber) {
        Validate.notNull(subscriber, "the 'subscriber' must not be null");
        subscribers.add(subscriber);
    }

    /**
     * @return true if last subscriber removed
     */
    public boolean unsubscribe(Subscriber subscriber) {
        Validate.notNull(subscriber, "the 'subscriber' must not be null");
        subscribers.remove(subscriber);
        return subscribers.isEmpty();
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
                jsonValidator.validate(jsonMessage,
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

        for (Subscriber subscriber : subscribers) {
            if (error != null || !isMessageExpired(message)) {
                subscriber.handleMessage(message, error);
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

    public interface Subscriber {
        void handleMessage(Message message, Exception exception);
    }
}
