package io.github.tcdl.msb.camel;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.camel.EndpointConfiguration;

import java.util.Optional;

/**
 * Created by rdro-tc on 09.08.16.
 */
public class MsbConfig {

    private static String MSB_SERVICE_CONFIG_ROOT = "msbConfig.serviceDetails";
    private static String MSB_BROKER_CONFIG_ROOT = "msbConfig.brokerConfig";

    private MsbConfig() {
    }

    public static Config from(EndpointConfiguration endPointConfiguration) {

        Config config = ConfigFactory.load();

        Optional<String> serviceName = Optional.ofNullable(endPointConfiguration.getParameter(MSB_SERVICE_CONFIG_ROOT + ".name"));
        Optional<String> instanceId = Optional.ofNullable(endPointConfiguration.getParameter(MSB_SERVICE_CONFIG_ROOT + ".instanceId"));

        Optional<String> host = Optional.ofNullable(endPointConfiguration.getParameter(MSB_BROKER_CONFIG_ROOT + ".host"));
        Optional<String> port = Optional.ofNullable(endPointConfiguration.getParameter(MSB_BROKER_CONFIG_ROOT + ".port"));
        Optional<String> virtualHost = Optional.ofNullable(endPointConfiguration.getParameter(MSB_BROKER_CONFIG_ROOT + ".virtualHost"));
        Optional<String> username = Optional.ofNullable(endPointConfiguration.getParameter(MSB_BROKER_CONFIG_ROOT + ".username"));
        Optional<String> password = Optional.ofNullable(endPointConfiguration.getParameter(MSB_BROKER_CONFIG_ROOT + ".password"));
        Optional<Boolean> durable = Optional.ofNullable(endPointConfiguration.getParameter(MSB_BROKER_CONFIG_ROOT + ".durable"));

        if (serviceName.isPresent()) {
            config = config.withValue(MSB_SERVICE_CONFIG_ROOT + ".name", ConfigValueFactory.fromAnyRef(serviceName.get()));
        }

        if (instanceId.isPresent()) {
            config = config.withValue(MSB_SERVICE_CONFIG_ROOT + ".instanceId", ConfigValueFactory.fromAnyRef(instanceId.get()));
        }

        if (host.isPresent()) {
            config = config.withValue(MSB_BROKER_CONFIG_ROOT + ".host", ConfigValueFactory.fromAnyRef(host.get()));
        }

        if (port.isPresent()) {
            config = config.withValue(MSB_BROKER_CONFIG_ROOT + ".port", ConfigValueFactory.fromAnyRef(port.get()));
        }

        if (virtualHost.isPresent()) {
            config = config.withValue(MSB_BROKER_CONFIG_ROOT + ".virtualHost", ConfigValueFactory.fromAnyRef(virtualHost.get()));
        }

        if (username.isPresent()) {
            config = config.withValue(MSB_BROKER_CONFIG_ROOT + ".username", ConfigValueFactory.fromAnyRef(username.get()));
        }

        if (password.isPresent()) {
            config = config.withValue(MSB_BROKER_CONFIG_ROOT + ".password", ConfigValueFactory.fromAnyRef(password.get()));
        }

        if (durable.isPresent()) {
            config = config.withValue(MSB_BROKER_CONFIG_ROOT + ".durable", ConfigValueFactory.fromAnyRef(durable.get()));
        }

        return config;
    }
}
