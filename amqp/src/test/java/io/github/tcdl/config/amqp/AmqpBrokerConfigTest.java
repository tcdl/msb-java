package io.github.tcdl.config.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import io.github.tcdl.config.amqp.AmqpBrokerConfig.AmqpBrokerConfigBuilder;
import io.github.tcdl.exception.ConfigurationException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@RunWith(MockitoJUnitRunner.class)
public class AmqpBrokerConfigTest {

    final String host = "127.0.0.1";
    final int port = 5672;
    final String username = "user";
    final String password = "pwd";
    final String virtualHost = "127.10.10.10";
    final String groupId = "msb-java";
    final boolean durable = false;
    final int consumerThreadPoolSize = 5;
    final int consumerThreadPoolQueueCapacity = 20;

    @Test
    public void testBuildAmqpBrokerConfig() {
        String configStr = "config.amqp {"
                + " host = \"" + host + "\"\n"
                + " port = \"" + port + "\"\n"
                + " username = \"" + username + "\"\n"
                + " password = \"" + password + "\"\n"
                + " virtualHost = \"" + virtualHost + "\"\n"
                + " groupId = \"" + groupId + "\"\n"
                + " durable = " + durable + "\n"
                + " consumerThreadPoolSize = " + consumerThreadPoolSize + "\n"
                + " consumerThreadPoolQueueCapacity = " + consumerThreadPoolQueueCapacity + "\n"
                + "}";

        Config amqpConfig = ConfigFactory.parseString(configStr).getConfig("config.amqp");
        AmqpBrokerConfigBuilder brokerConfigBuilder = new AmqpBrokerConfigBuilder(amqpConfig);
        AmqpBrokerConfig brokerConfig = brokerConfigBuilder.build();

        assertEquals(brokerConfig.getHost(), host);
        assertEquals(brokerConfig.getPort(), port);
        assertEquals(brokerConfig.getGroupId(), groupId);
        assertEquals(brokerConfig.isDurable(), durable);
        assertEquals(brokerConfig.getConsumerThreadPoolSize(), consumerThreadPoolSize);
        assertEquals(brokerConfig.getConsumerThreadPoolQueueCapacity(), consumerThreadPoolQueueCapacity);

        assertEquals(brokerConfig.getUsername().get(), username);
        assertEquals(brokerConfig.getPassword().get(), password);
        assertEquals(brokerConfig.getVirtualHost().get(), virtualHost);
    }

    @Test
    public void testOptionalConfigurationOptions() {
        String configStr = "config.amqp {"
                + " host = \"" + host + "\"\n"
                + " port = \"" + port + "\"\n"
                + " groupId = \"" + groupId + "\"\n"
                + " durable = " + durable + "\n"
                + " consumerThreadPoolSize = " + consumerThreadPoolSize + "\n"
                + " consumerThreadPoolQueueCapacity = " + consumerThreadPoolQueueCapacity + "\n"
                + "}";

        Config amqpConfig = ConfigFactory.parseString(configStr).getConfig("config.amqp");
        
        AmqpBrokerConfigBuilder brokerConfigBuilder = new AmqpBrokerConfigBuilder(amqpConfig);
        AmqpBrokerConfig brokerConfig = brokerConfigBuilder.build();

        //Verify empty optional values
        assertFalse(brokerConfig.getUsername().isPresent());
        assertFalse(brokerConfig.getPassword().isPresent());
        assertFalse(brokerConfig.getVirtualHost().isPresent());
    }

    @Test
    public void testHostConfigurationOption() {
        String configStr = "config.amqp {"
                + " port = \"" + port + "\"\n"
                + " username = \"" + username + "\"\n"
                + " password = \"" + password + "\"\n"
                + " virtualHost = \"" + virtualHost + "\"\n"
                + " groupId = \"" + groupId + "\"\n"
                + " durable = " + durable + "\n"
                + " consumerThreadPoolSize = " + consumerThreadPoolSize + "\n"
                + " consumerThreadPoolQueueCapacity = " + consumerThreadPoolQueueCapacity + "\n"
                + "}";

        testMandatoryConfigurationOption(configStr, "host");
    }

    @Test
    public void testPortConfigurationOption() {
        String configStr = "config.amqp {"
                + " host = \"" + host + "\"\n"
                + " username = \"" + username + "\"\n"
                + " password = \"" + password + "\"\n"
                + " virtualHost = \"" + virtualHost + "\"\n"
                + " groupId = \"" + groupId + "\"\n"
                + " durable = " + durable + "\n"
                + " consumerThreadPoolSize = " + consumerThreadPoolSize + "\n"
                + " consumerThreadPoolQueueCapacity = " + consumerThreadPoolQueueCapacity + "\n"
                + "}";

        testMandatoryConfigurationOption(configStr, "port");
    }

    @Test
    public void testGroupIdConfigurationOption() {
        String configStr = "config.amqp {"
                + " host = \"" + host + "\"\n"
                + " port = \"" + port + "\"\n"
                + " username = \"" + username + "\"\n"
                + " password = \"" + password + "\"\n"
                + " virtualHost = \"" + virtualHost + "\"\n"
                + " durable = " + durable + "\n"
                + " consumerThreadPoolSize = " + consumerThreadPoolSize + "\n"
                + " consumerThreadPoolQueueCapacity = " + consumerThreadPoolQueueCapacity + "\n"
                + "}";

        testMandatoryConfigurationOption(configStr, "groupId");
    }

    @Test
    public void testDurableConfigurationOption() {
        String configStr = "config.amqp {"
                + " host = \"" + host + "\"\n"
                + " port = \"" + port + "\"\n"
                + " username = \"" + username + "\"\n"
                + " password = \"" + password + "\"\n"
                + " virtualHost = \"" + virtualHost + "\"\n"
                + " groupId = \"" + groupId + "\"\n"
                + " consumerThreadPoolSize = " + consumerThreadPoolSize + "\n"
                + " consumerThreadPoolQueueCapacity = " + consumerThreadPoolQueueCapacity + "\n"
                + "}";

        testMandatoryConfigurationOption(configStr, "durable");
    }

    @Test
    public void testConsumerThreadPoolSizeConfigurationOption() {
        String configStr = "config.amqp {"
                + " host = \"" + host + "\"\n"
                + " port = \"" + port + "\"\n"
                + " username = \"" + username + "\"\n"
                + " password = \"" + password + "\"\n"
                + " virtualHost = \"" + virtualHost + "\"\n"
                + " groupId = \"" + groupId + "\"\n"
                + " durable = " + durable + "\n"
                + " consumerThreadPoolQueueCapacity = " + consumerThreadPoolQueueCapacity + "\n"
                + "}";

        testMandatoryConfigurationOption(configStr, "consumerThreadPoolSize");
    }

    @Test
    public void testConsumerThreadPoolQueueCapacityConfigurationOption() {
        String configStr = "config.amqp {"
                + " host = \"" + host + "\"\n"
                + " port = \"" + port + "\"\n"
                + " username = \"" + username + "\"\n"
                + " password = \"" + password + "\"\n"
                + " virtualHost = \"" + virtualHost + "\"\n"
                + " groupId = \"" + groupId + "\"\n"
                + " durable = " + durable + "\n"
                + " consumerThreadPoolSize = " + consumerThreadPoolSize + "\n"
                + "}";

        testMandatoryConfigurationOption(configStr, "consumerThreadPoolQueueCapacity");
    }

    private void testMandatoryConfigurationOption(String configStr, String path) {
        try {
            AmqpBrokerConfigBuilder builder = createConfigBuilder(configStr);
            builder.build();
            fail(String.format("Created AmqpBrokerConfig without Mandatory Configuration Option '%s'!", path));

        } catch (ConfigurationException expected) {
            assertTrue(String.format("Exception message doesn't mention '%s'?!", path),
                    expected.getMessage().contains(path));
        }
    }

    private AmqpBrokerConfigBuilder createConfigBuilder(String configStr) {
        Config amqpConfig = ConfigFactory.parseString(configStr).getConfig("config.amqp");
        return new AmqpBrokerConfigBuilder(amqpConfig);
    }

}
