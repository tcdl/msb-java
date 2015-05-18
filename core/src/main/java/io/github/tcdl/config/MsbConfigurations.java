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

	//AMQP specific properties
	private AmqpBrokerConfig amqpBrokerConf;

	//Kafka specific properties
	private KafkaBrokerConfig kafkaBrokerConf;
	
	private final ServiceDetails serviceDetails;

	private String schema;

	private BrokerAdapter msbBroker;

	public enum BrokerAdapter {
		AMQP, REDIS, KAFKA
	};

	private MsbConfigurations() {
		Config config = ConfigFactory.load().getConfig("msbConfig");

		this.serviceDetails = new ServiceDetails.ServiceDetailsBuilder(config.getConfig("serviceDetails")).build();		

		initBrokerConfigurations(config);
		initJsonSchema();
		setBrokerType(config);

		afterInit();
		log.info("MSB configuration {}", this);
	}

	public static MsbConfigurations msbConfiguration() {
		return INSTANCE;
	}	

	private void initBrokerConfigurations(Config config) {
		if (config.hasPath("config.amqp")) {
			this.amqpBrokerConf = new AmqpBrokerConfig.AmqpBrokerConfigBuilder(config.getConfig("config.amqp")).build();
		}
		// not in use
		if (config.hasPath("config.kafka")) {
			this.kafkaBrokerConf = new KafkaBrokerConfig.KafkaBrokerConfigBuilder(config.getConfig("config.kafka")).build();
		}
	}
	
	private void initJsonSchema() {
	    try {
            this.schema = IOUtils.toString(getClass().getResourceAsStream("/schema.js"));
        } catch (IOException e) {
            log.error("MSB configuration failed to load Json validation schema", this);
        }
    }  
	
	private void afterInit() {
		if (this.amqpBrokerConf != null && this.amqpBrokerConf.getGroupId() == null) {
			this.amqpBrokerConf.setGroupId(this.serviceDetails.getName());
		}
		// do the same for other broker configurations
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
	
	private void setBrokerType(Config config) {
        String brokerName = getString(config, "brokerAdapter ", "kafka").toUpperCase();
        this.msbBroker = BrokerAdapter.valueOf(brokerName);
    }

	public AmqpBrokerConfig getAmqpBrokerConf() {
		return amqpBrokerConf;
	}

	@Override
	public String toString() {
		return "MsbConfigurations [serviceDetails=" + serviceDetails + ", amqpBrokerConf=" + amqpBrokerConf
				+ ", kafkaBrokerConf=" + kafkaBrokerConf + ", schema=" + schema + ", msbBroker=" + msbBroker + "]";
	}

}
