package io.github.tcdl.msb.autoconfigure;

import io.github.tcdl.msb.config.MsbConfig;
import org.junit.After;
import org.junit.Test;
import org.springframework.boot.test.util.EnvironmentTestUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

import static org.junit.Assert.assertNotNull;

public class MsbAutoConfigurationTest {

    private AnnotationConfigApplicationContext context;

    @After
    public void tearDown() {
        if (this.context != null) {
            this.context.close();
        }
    }

    @Test
    public void testDefaultMsbConfig() {
        load(EmptyConfiguration.class, "msbConfig.serviceDetails.name=test-name");
        MsbConfig msbConfig = this.context.getBean(MsbConfig.class);
        assertNotNull(msbConfig);
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
