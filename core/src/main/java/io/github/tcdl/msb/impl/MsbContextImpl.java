package io.github.tcdl.msb.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.api.ObjectFactory;
import io.github.tcdl.msb.callback.MutableCallbackHandler;
import io.github.tcdl.msb.collector.CollectorManager;
import io.github.tcdl.msb.collector.CollectorManagerFactory;
import io.github.tcdl.msb.collector.TimeoutManager;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.message.MessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Clock;

/**
 * Specifies the context for MSB message processing.
 */
public class MsbContextImpl implements MsbContext {

    private static final Logger LOG = LoggerFactory.getLogger(MsbContextImpl.class);

    private final MsbConfig msbConfig;
    private volatile ObjectFactory objectFactory;
    private final MessageFactory messageFactory;
    private final ChannelManager channelManager;
    private final Clock clock;
    private final TimeoutManager timeoutManager;
    private final ObjectMapper payloadMapper;
    private final CollectorManagerFactory collectorManagerFactory;
    private final MutableCallbackHandler shutdownCallbackHandler;
    private volatile boolean isShutdownComplete = false;

    public MsbContextImpl(MsbConfig msbConfig, MessageFactory messageFactory, ChannelManager channelManager,
            Clock clock,
            TimeoutManager timeoutManager,
            ObjectMapper payloadMapper,
            CollectorManagerFactory collectorManagerFactory,
            MutableCallbackHandler shutdownCallbackHandler) {
        this.msbConfig = msbConfig;
        this.messageFactory = messageFactory;
        this.channelManager = channelManager;
        this.clock = clock;
        this.timeoutManager = timeoutManager;
        this.payloadMapper = payloadMapper;
        this.collectorManagerFactory = collectorManagerFactory;
        this.shutdownCallbackHandler = shutdownCallbackHandler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void shutdown() {
        if(!isShutdownComplete) {
            isShutdownComplete = true;
            LOG.info("Shutting down MSB context...");
            shutdownCallbackHandler.runCallbacks();
            timeoutManager.shutdown();
            channelManager.shutdown();
            LOG.info("MSB context has been shut down.");
        } else {
            LOG.warn("Trying to shutdown MsbContext several times");
        }
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
     * @return object of class {@link ObjectMapper} which will be used to deserialize/serialize payload of message
     */
    public ObjectMapper getPayloadMapper() {
        return payloadMapper;
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

    @Override
    public void addShutdownCallback(Runnable shutdownCallback) {
        shutdownCallbackHandler.add(shutdownCallback);
    }
}
