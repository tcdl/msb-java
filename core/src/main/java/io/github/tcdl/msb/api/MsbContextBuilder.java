package io.github.tcdl.msb.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JSR310Module;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.api.exception.MsbException;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.collector.CollectorManagerFactory;
import io.github.tcdl.msb.collector.TimeoutManager;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.impl.ObjectFactoryImpl;
import io.github.tcdl.msb.message.MessageFactory;
import io.github.tcdl.msb.monitor.DefaultChannelMonitorAgent;
import io.github.tcdl.msb.support.JsonValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;

/**
 * Create and initialize MsbContext object.
 * Usable as a start point for work with MSB. 
 */
public class MsbContextBuilder {
    
    private static final Logger LOG = LoggerFactory.getLogger(MsbContextBuilder.class);

    private Config config;
    private boolean enableShutdownHook;
    private boolean enableChannelMonitorAgent;
    private ObjectMapper payloadMapper;

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
     * @param enableShutdownHook if set to true will shutdown context regardless of
     * user will make a call to MsbContext.shutdown() from within client code and false otherwise
     * @return MsbContextBuilder
     */
    public MsbContextBuilder enableShutdownHook(boolean enableShutdownHook) {
        this.enableShutdownHook = enableShutdownHook;
        return this;
    }

    /**
     * Specifies if monitoring agent is enabled.
     * @param enableChannelMonitorAgent - true if monitoring agent is enabled and false otherwise
     * @return MsbContextBuilder
     */
    public MsbContextBuilder enableChannelMonitorAgent(boolean enableChannelMonitorAgent) {
        this.enableChannelMonitorAgent = enableChannelMonitorAgent;
        return this;
    }

    /**
     * Specifies payload object mapper to serialize/deserialize message payload
     * @param payloadMapper if not provided default object mapper will be used
     * @return MsbContextBuilder
     */
    public MsbContextBuilder withPayloadMapper(ObjectMapper payloadMapper) {
        this.payloadMapper = payloadMapper;
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
        MsbConfig msbConfig = new MsbConfig(config);
        ObjectMapper messageMapper = buildMessageMapper(payloadMapper);

        ChannelManager channelManager = new ChannelManager(msbConfig, clock, validator, messageMapper);
        MessageFactory messageFactory = new MessageFactory(msbConfig.getServiceDetails(), clock);
        TimeoutManager timeoutManager = new TimeoutManager(msbConfig.getTimerThreadPoolSize());
        CollectorManagerFactory collectorManagerFactory = new CollectorManagerFactory(channelManager);

        MsbContextImpl msbContext = new MsbContextImpl(msbConfig, messageFactory, channelManager, clock, timeoutManager, messageMapper, collectorManagerFactory);

        if (enableChannelMonitorAgent) {
            DefaultChannelMonitorAgent.start(msbContext);
        }
        
        if (enableShutdownHook) {
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

    public ObjectMapper buildMessageMapper(ObjectMapper payloadMapper) {
        ObjectMapper messageMapper = new ObjectMapper()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .registerModule(new JSR310Module());

        if (payloadMapper != null) {
            SimpleModule payloadModule = new SimpleModule()
                .addSerializer(Payload.class, new JsonSerializer<Payload>() {
                    @Override public void serialize(Payload value, JsonGenerator jsonGenerator, SerializerProvider provider) throws IOException {
                        jsonGenerator.writeString(payloadMapper.writeValueAsString(value));
                    }
                })
                .addDeserializer(Payload.class, new JsonDeserializer<Payload>() {
                    @Override public Payload deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException {
                        return payloadMapper.readValue(jsonParser.getValueAsString(), Payload.class);
                    }
                });

            messageMapper.registerModule(payloadModule);
        }

        return messageMapper;
    }
}
