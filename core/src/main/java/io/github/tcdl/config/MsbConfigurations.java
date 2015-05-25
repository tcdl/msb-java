package io.github.tcdl.config;

import static io.github.tcdl.config.ConfigurationUtil.getString;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class MsbConfigurations {

    public final Logger log = LoggerFactory.getLogger(getClass());

    private static MsbConfigurations INSTANCE = new MsbConfigurations();

    //Broker specific configuration
    private Config brokerConfig;

    private final ServiceDetails serviceDetails;

    private String schema;

    private BrokerAdapter msbBroker;

    public enum BrokerAdapter {
        AMQP, REDIS, KAFKA, LOCAL
    }

    private MsbConfigurations() {
        Config config = ConfigFactory.load().getConfig("msbConfig");

        this.serviceDetails = new ServiceDetails.ServiceDetailsBuilder(config.getConfig("serviceDetails")).build();
        this.schema = readJsonSchema();
        this.msbBroker = getBrokerType(config);
        this.brokerConfig = getBrokerConfig(config);

        log.info("MSB configuration {}", this);
    }

    public static MsbConfigurations msbConfiguration() {
        return INSTANCE;
    }
    
    private String readJsonSchema() {
        try {
            return IOUtils.toString(getClass().getResourceAsStream("/schema.js"));
        } catch (IOException e) {
            log.error("MSB configuration failed to load Json validation schema", this);
            return null;
        }
    }

    public ServiceDetails getServiceDetails() {
        return this.serviceDetails;
    }

    public String getSchema() {
        return this.schema;
    }

    public BrokerAdapter getBrokerType() {
        return this.msbBroker;
    }

    public Config getBrokerConfig() {
        return brokerConfig;
    }

    private BrokerAdapter getBrokerType(Config config) {
        String brokerName = getBrokerName(config).toUpperCase();
        return BrokerAdapter.valueOf(brokerName);
    }

    private Config getBrokerConfig(Config config) {
        String brokerName = getBrokerName(config).toLowerCase();
        String brokerConfigPath  = "config." + brokerName;
        return  config.hasPath(brokerConfigPath) ? config.getConfig(brokerConfigPath) : null;
    }
    
    private String getBrokerName(Config config) {
        return getString(config, "brokerAdapter", "amqp");
    }
    
    @Override
    public String toString() {
        return "MsbConfigurations [serviceDetails=" + serviceDetails + ", schema=" + schema 
                + ", msbBroker=" + msbBroker + ", brokerConfig=" + brokerConfig + "]";
    }

}
