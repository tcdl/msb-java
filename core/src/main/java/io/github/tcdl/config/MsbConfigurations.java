package io.github.tcdl.config;

import com.typesafe.config.Config;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Generic, non-adapter specific configuration of the msb system: service details and schema
 */
public class MsbConfigurations {

    public final Logger log = LoggerFactory.getLogger(getClass());

    private final ServiceDetails serviceDetails;

    private String schema;

    public MsbConfigurations(Config allconfig) {
        Config config = allconfig.getConfig("msbConfig");
        this.serviceDetails = new ServiceDetails.ServiceDetailsBuilder(config.getConfig("serviceDetails")).build();
        initJsonSchema();
        log.info("MSB configuration {}", this);
    }

    private void initJsonSchema() {
        try {
            this.schema = IOUtils.toString(getClass().getResourceAsStream("/schema.js"));
        } catch (IOException e) {
            log.error("MSB configuration failed to load Json validation schema", this);
        }
    }

    public ServiceDetails getServiceDetails() {
        return this.serviceDetails;
    }

    public String getSchema() {
        return this.schema;
    }

    @Override
    public String toString() {
        return "MsbConfigurations [serviceDetails=" + serviceDetails + ", schema=" + schema + "]";
    }
}
