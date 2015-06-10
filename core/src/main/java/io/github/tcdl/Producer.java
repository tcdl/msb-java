package io.github.tcdl;

import io.github.tcdl.adapters.ProducerAdapter;
import io.github.tcdl.exception.ChannelException;
import io.github.tcdl.exception.JsonConversionException;
import io.github.tcdl.messages.Message;
import io.github.tcdl.support.Utils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rdro on 4/23/2015.
 */
public class Producer {

    public static final Logger LOG = LoggerFactory.getLogger(Producer.class);

    private final ProducerAdapter rawAdapter;
    private final Callback<Message> messageHandler;

    public Producer(ProducerAdapter rawAdapter, String topic, Callback<Message> messageHandler) {
        LOG.debug("Creating producer for topic: {}", topic);
        Validate.notNull(rawAdapter, "the 'rawAdapter' must not be null");
        Validate.notNull(topic, "the 'topic' must not be null");
        Validate.notNull(messageHandler, "the 'messageHandler' must not be null");
        this.rawAdapter = rawAdapter;
        this.messageHandler = messageHandler;
    }

    public Producer publish(Message message) {
        try {
            String jsonMessage = Utils.toJson(message);
            LOG.debug("Publishing message to adapter : {}", jsonMessage);
            rawAdapter.publish(jsonMessage);
        } catch (ChannelException | JsonConversionException e) {
            LOG.error("Exception while message publish to adapter", e);
        }


        this.messageHandler.call(message);

        return this;
    }

}
