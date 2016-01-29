package io.github.tcdl.msb.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static io.github.tcdl.msb.config.ConfigurationUtil.*;

/**
 * {@link MsbConfig} class provides access to configuration properties.
 */
public class MsbConfig {

    public final Logger LOG = LoggerFactory.getLogger(getClass());

    //Broker Adapter Factory class. Represented with brokerAdapterFactory property from config 
    private final String brokerAdapterFactoryClass;

    //Broker specific configuration.
    private final Config brokerConfig;

    private final ServiceDetails serviceDetails;

    private final String schema;

    private final boolean validateMessage;

    private final int timerThreadPoolSize;

    private final boolean mdcLogging;

    private final String mdcLoggingKeyMessageTags;

    private final String mdcLoggingKeyCorrelationId;

    private final String mdcLoggingSplitTagsBy;

    public MsbConfig(Config loadedConfig) {
        Config config = loadedConfig.getConfig("msbConfig");

        Config serviceDetailsConfig = config.hasPath("serviceDetails") ? config.getConfig("serviceDetails") : ConfigFactory.empty();
        this.serviceDetails = new ServiceDetails.Builder(serviceDetailsConfig).build();
        this.schema = readJsonSchema();
        this.brokerAdapterFactoryClass = getBrokerAdapterFactory(config);
        this.brokerConfig = config.hasPath("brokerConfig") ? config.getConfig("brokerConfig") : ConfigFactory.empty();
        this.timerThreadPoolSize = getInt(config, "timerThreadPoolSize");
        this.validateMessage = getBoolean(config, "validateMessage");

        Config mdcLogging = config.getConfig("mdcLogging");
        Config mdcLoggingMessageKeys= mdcLogging.getConfig("messageKeys");

        this.mdcLogging = getBoolean(mdcLogging, "enabled");
        this.mdcLoggingSplitTagsBy = getOptionalString(mdcLogging, "splitTagsBy").orElse(null);
        this.mdcLoggingKeyMessageTags = getString(mdcLoggingMessageKeys, "messageTags");
        this.mdcLoggingKeyCorrelationId = getString(mdcLoggingMessageKeys, "correlationId");
        LOG.debug("Loaded {}", this);
    }

    private String readJsonSchema() {
        try {
            return IOUtils.toString(getClass().getResourceAsStream("/schema.js"));
        } catch (IOException e) {
            LOG.error("Failed to load Json validation schema", this);
            return null;
        }
    }

    private String getBrokerAdapterFactory(Config config) {
        return getString(config, "brokerAdapterFactory");
    }

    public ServiceDetails getServiceDetails() {
        return this.serviceDetails;
    }

    public String getSchema() {
        return this.schema;
    }

    public boolean isValidateMessage() {
        return validateMessage;
    }

    public Config getBrokerConfig() {
        return brokerConfig;
    }

    public String getBrokerAdapterFactory() {
        return brokerAdapterFactoryClass;
    }

    public int getTimerThreadPoolSize() {
        return timerThreadPoolSize;
    }

    public boolean isMdcLogging() {
        return mdcLogging;
    }

    public String getMdcLoggingKeyMessageTags() {
        return mdcLoggingKeyMessageTags;
    }

    public String getMdcLoggingKeyCorrelationId() {
        return mdcLoggingKeyCorrelationId;
    }

    public String getMdcLoggingSplitTagsBy() {
        return mdcLoggingSplitTagsBy;
    }

    @Override public String toString() {
        //please keep custom "brokerConfig" data
        return "MsbConfig{" +
                "brokerAdapterFactoryClass='" + brokerAdapterFactoryClass + '\'' +
                ", brokerConfig=" + brokerConfig +
                ", serviceDetails=" + serviceDetails +
                ", schema='" + schema + '\'' +
                ", validateMessage=" + validateMessage +
                ", timerThreadPoolSize=" + timerThreadPoolSize +
                ", mdcLogging=" + mdcLogging +
                ", mdcLoggingKeyMessageTags='" + mdcLoggingKeyMessageTags + '\'' +
                ", mdcLoggingKeyCorrelationId='" + mdcLoggingKeyCorrelationId + '\'' +
                ", mdcLoggingSplitTagsBy='" + mdcLoggingSplitTagsBy + '\'' +
                ", brokerConfig='" + brokerConfig.root().render() + '\'' +
                '}';
    }
}
