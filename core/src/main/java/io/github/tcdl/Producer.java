package io.github.tcdl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.adapters.AdapterFactory;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.events.EventHandler;
import io.github.tcdl.messages.Message;
import io.github.tcdl.support.Utils;
import io.github.tcdl.exception.ChannelException;
import io.github.tcdl.exception.JsonConversionException;

/**
 * Created by rdro on 4/23/2015.
 */
public class Producer {

    public static final Logger LOG = LoggerFactory.getLogger(Producer.class);

    private EventHandler messageHandler;
    private Adapter rawAdapter;

    public Producer(String topic, MsbConfigurations msbConfig) {
        LOG.info("Creating Producer for topic: {}", topic);
        this.rawAdapter = AdapterFactory.getInstance().createAdapter(msbConfig.getBrokerType(), topic);
    }

    public Producer publish(Message message) {
        Exception error = null;
        try {
            String jsonMessage = Utils.toJson(message);
            LOG.debug("Publishing message to adapter : {}", jsonMessage);
            rawAdapter.publish(jsonMessage);
        } catch (ChannelException | JsonConversionException e) {
            LOG.error("Exception while message publish to adapter", e);
            error = e;
        }

        if (messageHandler != null) {
            messageHandler.onEvent(message, error);
        }

        return this;
    }

    public Producer withMessageHandler(EventHandler messageHandler) {
        this.messageHandler = messageHandler;
        return this;
    }
}
