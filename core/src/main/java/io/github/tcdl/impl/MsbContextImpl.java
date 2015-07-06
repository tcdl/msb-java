package io.github.tcdl.impl;

import java.time.Clock;

import io.github.tcdl.ChannelManager;
import io.github.tcdl.TimeoutManager;
import io.github.tcdl.api.MsbContext;
import io.github.tcdl.api.ObjectFactory;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.message.MessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Specifies the context of the MSB message processing.
 */
public class MsbContextImpl implements MsbContext {

    private static final Logger LOG = LoggerFactory.getLogger(MsbContextImpl.class);

    private MsbConfigurations msbConfig;
    private ObjectFactory objectFactory;
    private MessageFactory messageFactory;
    private ChannelManager channelManager;
    private Clock clock;
    private TimeoutManager timeoutManager;

    public MsbContextImpl(MsbConfigurations msbConfig, MessageFactory messageFactory, ChannelManager channelManager, Clock clock, TimeoutManager timeoutManager) {
        this.msbConfig = msbConfig;
        this.messageFactory = messageFactory;
        this.channelManager = channelManager;
        this.clock = clock;
        this.timeoutManager = timeoutManager;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown() {
        LOG.info("Shutting down MSB context...");
        timeoutManager.shutdown();
        channelManager.shutdown();
        LOG.info("MSB context has been shut down.");
    }

    /**
     *
     * @return msb configuration ({@link MsbConfigurations})
     */
    public MsbConfigurations getMsbConfig() {
        return msbConfig;
    }

    /**
     *
     * @return message factory ({@link MessageFactory}) for incoming and outgoing messages
     */
    public MessageFactory getMessageFactory() {
        return messageFactory;
    }

    /**
     *
     * @return factory ({@link ChannelManager}) for creating producers and consumers
     */
    public ChannelManager getChannelManager() {
        return channelManager;
    }

    /**
     *
     * @return object of class {@link Clock} which MUST be used to obtain current time
     */
    public Clock getClock() {
        return clock;
    }

    /**
     *
     * @return object of class {@link TimeoutManager} which will be used for scheduling tasks
     */
    public TimeoutManager getTimeoutManager() {
        return timeoutManager;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectFactory getObjectFactory() {
        return objectFactory;
    }

    public void setObjectFactory(ObjectFactory objectFactory) {
        this.objectFactory = objectFactory;
    }
}
