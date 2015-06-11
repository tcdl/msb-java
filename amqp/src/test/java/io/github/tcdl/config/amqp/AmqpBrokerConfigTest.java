package io.github.tcdl.config.amqp;

import static org.junit.Assert.assertEquals;
import io.github.tcdl.config.amqp.AmqpBrokerConfig.AmqpBrokerConfigBuilder;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@RunWith(MockitoJUnitRunner.class)
public class AmqpBrokerConfigTest {

    @Test
    public void testBuildAmqpBrokerConfig() {
        final String host = "127.0.0.1";
        final int port = 5672;
        final String groupId = "msb-java";
        final boolean durable = Boolean.valueOf(false);
        final int consumerThreadPoolSize = 5;

        String configStr = "config.amqp {"
                    + " host = \"" + host + "\"\n"
                    + " port = \"" + 5672+ "\"\n"
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
        
    }

}
