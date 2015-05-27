package io.github.tcdl.adapters;

import io.github.tcdl.adapters.mock.AdapterFactory;
import io.github.tcdl.config.MsbConfigurations;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.junit.Test;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.*;

//ToDo:
public class AdapterFactoryLoaderTest {

    @Test
    public void testCreatedMockAdapter(){
        String configStr = "msbConfig {brokerAdapter = \"mock\"}";
        Config config = ConfigFactory.parseString(configStr);
        MsbConfigurations msbConfig = new MsbConfigurations(config);
        AdapterFactoryLoader loader = new AdapterFactoryLoader(msbConfig);
        MsbAdapterFactory adapterFactory = loader.getAdapterFactory();
        assertThat(adapterFactory, instanceOf(AdapterFactory.class));
    }
}