package io.github.tcdl.adapters;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.*;

public class AdapterFactoryLoaderTest {

    @Test
    public void testCreatedMockAdapter(){
        String configStr = "msbConfig {brokerAdapterFactory = \"io.github.tcdl.adapters.MockAdapterFactory\"}";
        Config config = ConfigFactory.parseString(configStr);
        AdapterFactoryLoader loader = new AdapterFactoryLoader(config);
        AdapterFactory adapterFactory = loader.getAdapterFactory();
        assertThat(adapterFactory, instanceOf(MockAdapterFactory.class));
    }
}