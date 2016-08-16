package io.github.tcdl.msb.camel.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.thomascook.status.client.MsbStatusClient;
import com.thomascook.status.client.reporter.ConfigReportingUtil;
import com.typesafe.config.Config;
import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.camel.MsbConsumer;
import org.apache.camel.Route;
import org.apache.camel.impl.RoutePolicySupport;

import java.util.Map;

/**
 * Created by rdro-tc on 16.08.16.
 */
public class HealthCheckPolicy extends RoutePolicySupport {

    private String healthCheckName;
    private HealthCheck healthCheck;
    private MsbStatusClient statusClient;

    public HealthCheckPolicy(String healthCheckName, HealthCheck healthCheck) {
        this.healthCheckName = healthCheckName;
        this.healthCheck = healthCheck;
    }

    @Override
    public void onStart(Route route) {
        MsbConsumer msbConsumer = (MsbConsumer)route.getConsumer();
        MsbContext msbContext = msbConsumer.getMsbContext();
        Config msbConfig = msbConsumer.getMsbConfig();

        Map<String, String> configMap = ConfigReportingUtil.configToMap(msbConfig);
        HealthCheckRegistry registry = new HealthCheckRegistry();
        registry.register(healthCheckName, healthCheck);

        statusClient = new MsbStatusClient(registry, msbContext, configMap);
        statusClient.start();
    }

    @Override
    public void onStop(Route route) {
        statusClient.stop();
    }
}
