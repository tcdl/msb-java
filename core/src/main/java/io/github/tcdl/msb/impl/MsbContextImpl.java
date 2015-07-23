package io.github.tcdl.msb.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.api.ObjectFactory;
import io.github.tcdl.msb.collector.CollectorManager;
import io.github.tcdl.msb.collector.CollectorManagerFactory;
import io.github.tcdl.msb.collector.TimeoutManager;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.message.MessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;

/**
 * Specifies the context of the MSB message processing.
 */
public class MsbContextImpl implements MsbContext {

    private static final Logger LOG = LoggerFactory.getLogger(MsbContextImpl.class);

    private MsbConfig msbConfig;
    private ObjectFactory objectFactory;
    private MessageFactory messageFactory;
    private ChannelManager channelManager;
    private Clock clock;
    private TimeoutManager timeoutManager;
    private ObjectMapper messageMapper;
    private CollectorManagerFactory collectorManagerFactory;

    public MsbContextImpl(MsbConfig msbConfig, MessageFactory messageFactory, ChannelManager channelManager,
            Clock clock,
            TimeoutManager timeoutManager,
            ObjectMapper messageMapper,
            CollectorManagerFactory collectorManagerFactory) {
        this.msbConfig = msbConfig;
        this.messageFactory = messageFactory;
        this.channelManager = channelManager;
        this.clock = clock;
        this.timeoutManager = timeoutManager;
        this.messageMapper = messageMapper;
        this.collectorManagerFactory = collectorManagerFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown() {
        LOG.info("Shutting down MSB context...");
        objectFactory.shutdown();
        timeoutManager.shutdown();
        channelManager.shutdown();
        LOG.info("MSB context has been shut down.");
    }

    /**
     * @return msb configuration ({@link MsbConfig})
     */
    public MsbConfig getMsbConfig() {
        return msbConfig;
    }

    /**
     * @return message factory ({@link MessageFactory}) for incoming and outgoing messages
     */
    public MessageFactory getMessageFactory() {
        return messageFactory;
    }

    /**
     * @return factory ({@link ChannelManager}) for creating producers and consumers
     */
    public ChannelManager getChannelManager() {
        return channelManager;
    }

    /**
     * @return object of class {@link Clock} which MUST be used to obtain current time
     */
    public Clock getClock() {
        return clock;
    }

    /**
     * @return object of class {@link TimeoutManager} which will be used for scheduling tasks
     */
    public TimeoutManager getTimeoutManager() {
        return timeoutManager;
    }

    /**
     * @return object of class {@link ObjectMapper} which will be used to deserialize/serialize message
     */
    public ObjectMapper getMessageMapper() {
        return messageMapper;
    }

    /**
     * @return object of class {@link CollectorManagerFactory} which will be used for creating and holding objects of class {@link CollectorManager}
     */
    public CollectorManagerFactory getCollectorManagerFactory() {
        return collectorManagerFactory;
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
