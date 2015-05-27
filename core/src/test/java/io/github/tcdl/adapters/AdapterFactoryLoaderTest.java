package io.github.tcdl.adapters;

import io.github.tcdl.adapters.mock.MockAdapterFactory;
import io.github.tcdl.config.MsbConfigurations;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.junit.Test;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.*;

//ToDo:
public class AdapterFactoryLoaderTest {

    @Test
    public void testCreatedMockAdapterByEmptyFactoryClassName(){
        String configStr = "msbConfig {}";
        Config config = ConfigFactory.parseString(configStr);
        MsbConfigurations msbConfig = new MsbConfigurations(config);
        AdapterFactoryLoader loader = new AdapterFactoryLoader(msbConfig);
        AdapterFactory adapterFactory = loader.getAdapterFactory();
        assertThat(adapterFactory, instanceOf(MockAdapterFactory.class));
    }
    
    @Test
    public void testCreatedMockAdapterByFactoryClassName(){
        String configStr = "msbConfig {brokerAdapterFactory = \"io.github.tcdl.adapters.mock.MockAdapterFactory\"}";
        Config config = ConfigFactory.parseString(configStr);
        MsbConfigurations msbConfig = new MsbConfigurations(config);
        AdapterFactoryLoader loader = new AdapterFactoryLoader(msbConfig);
        AdapterFactory adapterFactory = loader.getAdapterFactory();
        assertThat(adapterFactory, instanceOf(MockAdapterFactory.class));
    }

}