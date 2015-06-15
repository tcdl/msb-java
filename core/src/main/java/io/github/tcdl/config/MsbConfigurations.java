package io.github.tcdl.config;

import static io.github.tcdl.config.ConfigurationUtil.getString;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *  MsbConfigurations class provides access to configuration properties
 */
public class MsbConfigurations {

    public final Logger LOG = LoggerFactory.getLogger(getClass());

    //Broker Adapter Factory class. Represented with brokerAdapterFactory property from config 
    private String brokerAdapterFactoryClass;

    //Broker specific configuration.
    private Config brokerConfig;

    private final ServiceDetails serviceDetails;

    private String schema;

    public MsbConfigurations(Config loadedConfig) {
        Config config = loadedConfig.getConfig("msbConfig");

        Config serviceDetailsConfig = config.hasPath("serviceDetails") ? config.getConfig("serviceDetails") : ConfigFactory.empty();
        this.serviceDetails = new ServiceDetails.ServiceDetailsBuilder(serviceDetailsConfig).build();
        this.schema = readJsonSchema();
        this.brokerAdapterFactoryClass = getBrokerAdapterFactory(config);
        this.brokerConfig = config.hasPath("brokerConfig") ? config.getConfig("brokerConfig") : ConfigFactory.empty();

        LOG.debug("MSB configuration {}", this);
    }

    private String readJsonSchema() {
        try {
            return IOUtils.toString(getClass().getResourceAsStream("/schema.js"));
        } catch (IOException e) {
            LOG.error("MSB configuration failed to load Json validation schema", this);
            return null;
        }
    }

    private String getBrokerAdapterFactory(Config config) {
        return getString(config, "brokerAdapterFactory", "");
    }
    
    public ServiceDetails getServiceDetails() {
        return this.serviceDetails;
    }

    public String getSchema() {
        return this.schema;
    }

    public Config getBrokerConfig() {
        return brokerConfig;
    }

    public String getBrokerAdapterFactory() {
        return brokerAdapterFactoryClass;
    }

    @Override
    public String toString() {
        return "MsbConfigurations [serviceDetails=" + serviceDetails + ", schema=" + schema 
                + ", brokerAdapterFactory=" + brokerAdapterFactoryClass + ", brokerConfig=" + brokerConfig + "]";
    }

}
