package io.github.tcdl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.support.JsonValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.tcdl.exception.MsbException;

import java.time.Clock;

/**
 * {@link  MsbContext} class contains all singleton beans required for MSB
 *
 * Created by rdro on 5/27/2015.
 */
public class MsbContext {

    private static final Logger LOG = LoggerFactory.getLogger(MsbContext.class);

    private MsbConfigurations msbConfig;
    private MessageFactory messageFactory;
    private ChannelManager channelManager;
    private Clock clock;
    private TimeoutManager timeoutManager;

    public MsbContext(MsbConfigurations msbConfig, MessageFactory messageFactory, ChannelManager channelManager, Clock clock, TimeoutManager timeoutManager) {
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

    public void setMsbConfig(MsbConfigurations msbConfig) {
        this.msbConfig = msbConfig;
    }

    public MessageFactory getMessageFactory() {
        return messageFactory;
    }

    public void setMessageFactory(MessageFactory messageFactory) {
        this.messageFactory = messageFactory;
    }

    public ChannelManager getChannelManager() {
        return channelManager;
    }

    public void setChannelManager(ChannelManager channelManager) {
        this.channelManager = channelManager;
    }

    public Clock getClock() {
        return clock;
    }

    public void setClock(Clock clock) {
        this.clock = clock;
    }

    public TimeoutManager getTimeoutManager() {
        return timeoutManager;
    }

    public static class MsbContextBuilder {
        private boolean withShutdownHook;

        public MsbContextBuilder withShutdownHook(boolean withShutdownHook) {
            this.withShutdownHook = withShutdownHook;
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
