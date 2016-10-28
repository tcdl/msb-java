package io.github.tcdl.msb.autoconfigure;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.tcdl.msb.adapters.amqp.AmqpAdapterFactory;
import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.config.MsbConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.boot.test.util.EnvironmentTestUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MsbAutoConfigurationTest {

    private AnnotationConfigApplicationContext context;

    @Mock
    private AmqpAdapterFactory amqpAdapterFactoryMock;

    @Before
    public void init() {
        when(amqpAdapterFactoryMock.init()).thenReturn(); //TODO
    }

    @After
    public void tearDown() {
        if (this.context != null) {
            this.context.close();
        }
    }

    @Test
    public void testDefaultMsbConfig() {
        Config config = ConfigFactory.load("defaultMsbConfig");
        load(EmptyConfiguration.class);
        MsbConfig msbConfig = this.context.getBean(MsbConfig.class);

        assertNotNull(msbConfig);
        assertNotNull(msbConfig.getServiceDetails().getName());
        assertEquals(config.getString("msbConfig.serviceDetails.version"), msbConfig.getServiceDetails().getVersion());
        assertEquals(config.getString("msbConfig.brokerAdapterFactory"), msbConfig.getBrokerAdapterFactory());
        assertEquals(config.getInt("msbConfig.timerThreadPoolSize"), msbConfig.getTimerThreadPoolSize());
        assertEquals(config.getInt("msbConfig.threadingConfig.consumerThreadPoolSize"), msbConfig.getConsumerThreadPoolSize());
        assertEquals(config.getInt("msbConfig.threadingConfig.consumerThreadPoolQueueCapacity"), msbConfig.getConsumerThreadPoolQueueCapacity());
        assertEquals(config.getBoolean("msbConfig.validateMessage"), msbConfig.isValidateMessage());
        assertEquals(config.getBoolean("msbConfig.brokerConfig.durable"), msbConfig.getBrokerConfig().getBoolean("durable"));
    }

    @Test
    public void testOverrideMsbConfigParams() {
        load(EmptyConfiguration.class, "msbConfig.serviceDetails.name=test-name", "msbConfig.threadingConfig.consumerThreadPoolSize=100", "msbConfig.brokerConfig.host=192.168.0.1");
        MsbConfig msbConfig = this.context.getBean(MsbConfig.class);

        assertEquals("test-name", msbConfig.getServiceDetails().getName());
        assertEquals(100, msbConfig.getConsumerThreadPoolSize());
        assertEquals("192.168.0.1", msbConfig.getBrokerConfig().getString("host"));
    }

    @Test
    public void testDefaultMsbContextCreation() {
        load(EmptyConfiguration.class);
        MsbContext msbContext = this.context.getBean(MsbContext.class);
        assertNotNull(msbContext);
    }


    @Configuration
    static class EmptyConfiguration {}

    private void load(Class<?> config, String... environment) {
        AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(applicationContext, environment);
        applicationContext.register(config);
        applicationContext.register(MsbConfigAutoConfiguration.class, MsbContextAutoConfiguration.class);
        applicationContext.refresh();
        this.context = applicationContext;
    }
}
