package io.github.tcdl.msb.autoconfigure;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {MsbConfigAutoConfiguration.class, MsbContextAutoConfiguration.class})
@TestPropertySource(properties = "msb-config.enabled=false")
public class MsbAutoConfigurationDisableTest {

    @Autowired
    private ApplicationContext context;

    @Test
    public void shouldDisableAutoConfigurationByFeatureFlag() {
        assertTrue(context.getBeansOfType(MsbConfigAutoConfiguration.class).isEmpty());
        assertTrue(context.getBeansOfType(MsbContextAutoConfiguration.class).isEmpty());
    }

}
