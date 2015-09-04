package io.github.tcdl.msb.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import io.github.tcdl.msb.api.exception.ConfigurationException;

import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ServiceDetailsTest {
    final String name = "test_msb";
    final String version = "1.0.1";
    final String instanceId = "msbd06a-ed59-4a39-9f95-811c5fb6ab87";

    @Test
    public void testServiceDetailsAll() {
        String serviceDetailsConfigStr = "serviceDetails = {name = \"" + name + "\", version = \"" + version + "\", instanceId = \"" + instanceId + "\"}";
        Config config = ConfigFactory.parseString(serviceDetailsConfigStr);
        ServiceDetails serviceDetails = new ServiceDetails.Builder(config.getConfig("serviceDetails")).build();

        assertEquals("expect \"" + name + "\" as a name value", name, serviceDetails.getName());
        assertEquals("expect \"" + version + "\" as a vesrion value", version, serviceDetails.getVersion());
        assertEquals("expect \"" + instanceId + "\" as an instanceId value", instanceId, serviceDetails.getInstanceId());

        assertNotNull("expect Hostname is not null", serviceDetails.getHostname());
        assertNotNull("expect Ip is not null", serviceDetails.getIp());
        assertNotNull("expect Pid is not null", serviceDetails.getPid());
    }

    @Test(expected = ConfigurationException.class)
    public void testServiceDetailsWithoutName() {
        String serviceDetailsConfigStr = "serviceDetails = {version = \"" + version + "\", instanceId = \"" + instanceId + "\"}";
        Config config = ConfigFactory.parseString(serviceDetailsConfigStr);
        new ServiceDetails.Builder(config.getConfig("serviceDetails")).build();
    }

    @Test(expected = ConfigurationException.class)
    public void testServiceDetailsWithoutVersion() {
        String serviceDetailsConfigStr = "serviceDetails = {name = \"" + name + "\", instanceId = \"" + instanceId + "\"}";
        Config config = ConfigFactory.parseString(serviceDetailsConfigStr);
        new ServiceDetails.Builder(config.getConfig("serviceDetails")).build();
    }

    @Test
    public void testServiceDetailsWithoutInstanceId() {
        String serviceDetailsConfigStr = "serviceDetails = {name = \"" + name + "\", version = \"" + version + "\"}";
        Config config = ConfigFactory.parseString(serviceDetailsConfigStr);
        ServiceDetails serviceDetails = new ServiceDetails.Builder(config.getConfig("serviceDetails")).build();

        assertEquals("expect \"" + name + "\" as a name value", name, serviceDetails.getName());
        assertEquals("expect \"" + version + "\" as a vesrion value", version, serviceDetails.getVersion());
        assertNotNull("expect \"" + instanceId + "\" as an instanceId value", serviceDetails.getInstanceId());

        String instanceId1 = serviceDetails.getInstanceId();
        String instanceId2 = serviceDetails.getInstanceId();
        assertEquals("expect stabel instanceId value", instanceId1, instanceId2);
    }

}
