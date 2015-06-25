package io.github.tcdl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.monitor.DefaultChannelMonitorAgent;
import io.github.tcdl.support.JsonValidator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.tcdl.exception.MsbException;

import java.time.Clock;

/**
 * {@link  MsbContext} specifies the context of the MSB message processing. Creates singleton beans required for MSB.
 */
public class MsbContext {

    private static final Logger LOG = LoggerFactory.getLogger(MsbContext.class);

    private MsbConfigurations msbConfig;
    private MessageFactory messageFactory;
    private ChannelManager channelManager;
    private Clock clock;
    private TimeoutManager timeoutManager;

    protected MsbContext(MsbConfigurations msbConfig, MessageFactory messageFactory, ChannelManager channelManager, Clock clock, TimeoutManager timeoutManager) {
        this.msbConfig = msbConfig;
        this.messageFactory = messageFactory;
        this.channelManager = channelManager;
        this.clock = clock;
        this.timeoutManager = timeoutManager;
    }

    /**
     * Gracefully shuts down the current context.
     * This methods is not guaranteed to be THREAD-SAFE and is not intended to be executed in parallel from different threads
     */
    public void shutdown() {
        LOG.info("Shutting down MSB context...");
        timeoutManager.shutdown();
        channelManager.shutdown();
        LOG.info("MSB context has been shut down.");
    }

    public MsbConfigurations getMsbConfig() {
        return msbConfig;
    }

    public MessageFactory getMessageFactory() {
        return messageFactory;
    }

    public ChannelManager getChannelManager() {
        return channelManager;
    }

    public Clock getClock() {
        return clock;
    }

    public TimeoutManager getTimeoutManager() {
        return timeoutManager;
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
         * Create MsbContext and initialize it with Config from reference.conf
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

            MsbContext msbContext = new MsbContext(msbConfig, messageFactory, channelManager, clock, timeoutManager);

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

            return msbContext;
        }
    }
}
