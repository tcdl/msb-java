package io.github.tcdl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.github.tcdl.api.MsbContext;
import io.github.tcdl.api.ObjectFactory;
import io.github.tcdl.api.exception.MsbException;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.message.MessageFactory;
import io.github.tcdl.monitor.DefaultChannelMonitorAgent;
import io.github.tcdl.support.JsonValidator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;

/**
 * Specifies the context of the MSB message processing.
 * Method {@link MsbContextBuilder#withShutdownHook} is used for specifying shutdown hook during JVM exit.
 * Method {@link MsbContextBuilder#withDefaultChannelMonitorAgent} is used for enable or disable monitoring agent.
 * Method {@link #shutdown} is used for shut down context.
 */
public class MsbContextImpl implements MsbContext {

    private static final Logger LOG = LoggerFactory.getLogger(MsbContextImpl.class);

    private MsbConfigurations msbConfig;
    private ObjectFactoryImpl objectFactory;
    private MessageFactory messageFactory;
    private ChannelManager channelManager;
    private Clock clock;
    private TimeoutManager timeoutManager;

    protected MsbContextImpl(MsbConfigurations msbConfig, MessageFactory messageFactory, ChannelManager channelManager, Clock clock, TimeoutManager timeoutManager) {
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

    private void setObjectFactory(ObjectFactoryImpl objectFactory) {
        this.objectFactory = objectFactory;
    }

    public static class MsbContextBuilder {
        private boolean withShutdownHook;
        private boolean withDefaultChannelMonitorAgent;

        /**
         * Specifies if to shutdown current context during JVM exit. If set to true will shutdown context regardless of
         * user will make a call to MsbContext.shutdown() from within client code.
         */
        public MsbContextBuilder withShutdownHook(boolean withShutdownHook) {
            this.withShutdownHook = withShutdownHook;
            return this;
        }

        /**
         * Specifies if monitoring agent is enabled.
         *
         */
        public MsbContextBuilder withDefaultChannelMonitorAgent(boolean withDefaultChannelMonitorAgent) {
            this.withDefaultChannelMonitorAgent = withDefaultChannelMonitorAgent;
            return this;
        }
        
        /**
         * Create implememntation of {@link MsbContext} and initialize it with configuration from reference.conf(property file inside MSB library)
         * or application.conf, which will override library properties
         * This is environment where microservice will be run, it holds all necessary information such as
         * bus configuration, service details, schema for incoming and outgoing messages, factory for building requests
         * and responses etc.
         * @return MsbContext
         * @throws MsbException if an error happens during initialization
         */
        public MsbContext build() {
            Clock clock = Clock.systemDefaultZone();
            Config config = ConfigFactory.load();
            JsonValidator validator = new JsonValidator();
            MsbConfigurations msbConfig = new MsbConfigurations(config);
            ChannelManager channelManager = new ChannelManager(msbConfig, clock, validator);
            MessageFactory messageFactory = new MessageFactory(msbConfig.getServiceDetails(), clock);
            TimeoutManager timeoutManager = new TimeoutManager(msbConfig.getTimerThreadPoolSize());

            MsbContextImpl msbContext = new MsbContextImpl(msbConfig, messageFactory, channelManager, clock, timeoutManager);

            if (withDefaultChannelMonitorAgent) {
                DefaultChannelMonitorAgent.start(msbContext);
            }
            
            if (withShutdownHook) {
                Runtime.getRuntime().addShutdownHook(new Thread("MSB shutdown hook") {
                    @Override
                    public void run() {
                        LOG.info("Invoking shutdown hook...");
                        msbContext.shutdown();
                        LOG.info("Shutdown hook has completed.");
                    }
                });
            }

            ObjectFactoryImpl objectFactory = new ObjectFactoryImpl(msbContext); 
            msbContext.setObjectFactory(objectFactory);
            
            return msbContext;
        }
    }
}
