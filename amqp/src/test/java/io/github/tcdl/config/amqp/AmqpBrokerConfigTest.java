package io.github.tcdl.config.amqp;

import static org.junit.Assert.assertEquals;
import io.github.tcdl.config.amqp.AmqpBrokerConfig.AmqpBrokerConfigBuilder;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@RunWith(MockitoJUnitRunner.class)
public class AmqpBrokerConfigTest {

    @Test
    public void testBuildAmqpBrokerConfig() {
        final String host = "127.0.0.1";
        final int port = 5672;
        final String userName = "user";
        final String password = "pwd";
        final String virtualHost = "127.10.10.10"; 
        final String groupId = "msb-java";
        final boolean durable = Boolean.valueOf(false);
        final int consumerThreadPoolSize = 5;

        String configStr = "config.amqp {"
                    + " host = \"" + host + "\"\n"
                    + " port = \"" + 5672+ "\"\n"
                    + " userName = \"" + userName + "\"\n"
                    + " password = \"" + password + "\"\n"
                    + " virtualHost = \"" + virtualHost + "\"\n"
                    + " groupId = \"" + groupId + "\"\n"
                    + " durable = " + durable + "\n"
                    + " consumerThreadPoolSize = " + consumerThreadPoolSize + "\n" 
                    + "}";
        
        Config amqpConfig = ConfigFactory.parseString(configStr).getConfig("config.amqp");;
        AmqpBrokerConfigBuilder brokerConfigBuilder = new AmqpBrokerConfigBuilder(amqpConfig);
        AmqpBrokerConfig brokerConfig = brokerConfigBuilder.build();
        
        assertEquals(brokerConfig.getHost(), host);
        assertEquals(brokerConfig.getPort(), port);
        assertEquals(brokerConfig.getGroupId(), groupId);
        assertEquals(brokerConfig.isDurable(), durable);
        assertEquals(brokerConfig.getConsumerThreadPoolSize(), consumerThreadPoolSize);
        
        assertEquals(brokerConfig.getUserName().get(), userName);
        assertEquals(brokerConfig.getPassword().get(), password);
        assertEquals(brokerConfig.getVirtualHost().get(), virtualHost);
    }

    @Test
    public void testBuildEmptyAmqpBrokerConfig() {

        String configStr = "config.amqp {}";
        
        Config amqpConfig = ConfigFactory.parseString(configStr).getConfig("config.amqp");;
        AmqpBrokerConfigBuilder brokerConfigBuilder = new AmqpBrokerConfigBuilder(amqpConfig);
        AmqpBrokerConfig brokerConfig = brokerConfigBuilder.build();
        
        assertEquals(brokerConfig.getHost(), AmqpBrokerConfig.HOST_DEFAULT);
        assertEquals(brokerConfig.getPort(), AmqpBrokerConfig.PORT_DEFAULT);
        assertEquals(brokerConfig.getGroupId(), AmqpBrokerConfig.GROUP_ID_DEFAULT);
        assertEquals(brokerConfig.isDurable(), AmqpBrokerConfig.DURABLE_DEFAULT);
        assertEquals(brokerConfig.getConsumerThreadPoolSize(), AmqpBrokerConfig.CONSUMER_THREAD_POOL_SIZE_DEFAULT);
        
        org.junit.Assert.assertTrue(!brokerConfig.getUserName().isPresent());
        org.junit.Assert.assertTrue(!brokerConfig.getPassword().isPresent());
        org.junit.Assert.assertTrue(!brokerConfig.getUserName().isPresent());
    }

}
