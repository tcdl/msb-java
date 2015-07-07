package io.github.tcdl.api;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.tcdl.ChannelManager;
import io.github.tcdl.collector.CollectorManagerFactory;
import io.github.tcdl.collector.TimeoutManager;
import io.github.tcdl.api.exception.MsbException;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.impl.MsbContextImpl;
import io.github.tcdl.impl.ObjectFactoryImpl;
import io.github.tcdl.message.MessageFactory;
import io.github.tcdl.monitor.DefaultChannelMonitorAgent;
import io.github.tcdl.support.JsonValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;

/**
 * Create and initialize MsbContext object.
 * Usable as a start point for work with MSB. 
 */
public class MsbContextBuilder {
    
    private static final Logger LOG = LoggerFactory.getLogger(MsbContextBuilder.class);

    private Config config;
    private boolean withShutdownHook;
    private boolean withDefaultChannelMonitorAgent;

    public MsbContextBuilder() {
        super();
    }

    /**
     * Overrides default configuration from reference.conf with given type-safe configuration
     * @param config type-safe configuration bean
     * @return MsbContextBuilder
     */
    public MsbContextBuilder withConfig(Config config) {
        this.config = config;
        return this;
    }

    /**
     * Specifies if to shutdown current context during JVM exit. 
     * @param withShutdownHook if set to true will shutdown context regardless of
     * user will make a call to MsbContext.shutdown() from within client code and false otherwise
     * @return MsbContextBuilder
     */
    public MsbContextBuilder withShutdownHook(boolean withShutdownHook) {
        this.withShutdownHook = withShutdownHook;
        return this;
    }

    /**
     * Specifies if monitoring agent is enabled.
     * @param withDefaultChannelMonitorAgent - true if monitoring agent is enabled and false otherwise 
     * @return MsbContextBuilder
     */
    public MsbContextBuilder withDefaultChannelMonitorAgent(boolean withDefaultChannelMonitorAgent) {
        this.withDefaultChannelMonitorAgent = withDefaultChannelMonitorAgent;
        return this;
    }
    
    /**
     * Create implementation of {@link MsbContext}
     * Can be initialized with configuration from reference.conf(property file inside MSB library) or application.conf,
     * which will override library properties. Also configuration can be specified directly with withConfig method
     * This is environment where microservice will be run, it holds all necessary information such as
     * bus configuration, service details, schema for incoming and outgoing messages, factory for building requests
     * and responses etc.
     * @return MsbContext
     * @throws MsbException if an error happens during initialization
     */
    public MsbContext build() {
        Clock clock = Clock.systemDefaultZone();
        JsonValidator validator = new JsonValidator();
        if (config == null) {
            config = ConfigFactory.load();
        }
        MsbConfigurations msbConfig = new MsbConfigurations(config);
        ChannelManager channelManager = new ChannelManager(msbConfig, clock, validator);
        MessageFactory messageFactory = new MessageFactory(msbConfig.getServiceDetails(), clock);
        TimeoutManager timeoutManager = new TimeoutManager(msbConfig.getTimerThreadPoolSize());
        CollectorManagerFactory collectorManagerFactory = new CollectorManagerFactory(channelManager);

        MsbContextImpl msbContext = new MsbContextImpl(msbConfig, messageFactory, channelManager, clock, timeoutManager, collectorManagerFactory);

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

        ObjectFactory objectFactory = new ObjectFactoryImpl(msbContext); 
        msbContext.setObjectFactory(objectFactory);
        
        return msbContext;
    }
}
