package io.github.tcdl.adapters.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.tcdl.adapters.ConsumerAdapter;
import io.github.tcdl.adapters.ProducerAdapter;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.amqp.AmqpBrokerConfig;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;

public class AmqpAdapterFactoryTest {
    final String host = "127.0.0.1";
    final int port = 5672;
    final String username = "user";
    final String password = "pwd";
    final String virtualHost = "127.10.10.10"; 
    final String groupId = "msb-java";
    final boolean durable = false;
    final int consumerThreadPoolSize = 5;
    final int consumerThreadPoolQueueCapacity = 20;
    
    AmqpBrokerConfig amqpConfig;
    AmqpAdapterFactory amqpAdapterFactory;
    AmqpConnectionManager mockConnectionManager;
    ConnectionFactory mockConnectionFactory;
    Connection mockConnection;
    ExecutorService mockConsumerThreadPool;
    MsbConfigurations msbConfigurations;
    
    @Before
    public void setUp() {
        String configStr = "msbConfig {"
                + "  timerThreadPoolSize = 1\n"
                + "  brokerAdapterFactory = \"io.github.tcdl.adapters.amqp.AmqpAdapterFactory\" \n"
                + "  validateMessage = true\n"
                + "  serviceDetails = {"
                + "     name = \"test_msb\" \n"
                + "     version = \"1.0.1\" \n"
                + "     instanceId = \"msbd06a-ed59-4a39-9f95-811c5fb6ab87\" \n"
                + "  } \n"
                + "}";
        Config msbConfig = ConfigFactory.parseString(configStr);
        msbConfigurations = new MsbConfigurations(msbConfig); 

        mockConnectionFactory = mock(ConnectionFactory.class);
        mockConnection = mock(Connection.class);
        mockConnectionManager = mock(AmqpConnectionManager.class);
        mockConsumerThreadPool = mock(ExecutorService.class);
        
        //Define conditions for ExecutorService termination
        try {
            when(mockConsumerThreadPool.awaitTermination(anyInt(), any(TimeUnit.class))).thenReturn(true);
        } catch (InterruptedException e) {
            fail("Can't create mockConsumerThreadPool");
        }
        
        amqpConfig = new AmqpBrokerConfig(host, port, 
                Optional.of(username), Optional.of(password), Optional.of(virtualHost), groupId, durable, consumerThreadPoolSize, consumerThreadPoolQueueCapacity);
        
        amqpAdapterFactory = new AmqpAdapterFactory() {
            @Override
            public ProducerAdapter createProducerAdapter(String topic) {
                return new AmqpProducerAdapter(topic, mockConnectionManager);
            }

            @Override
            public ConsumerAdapter createConsumerAdapter(String topic) {
                return new AmqpConsumerAdapter(topic, amqpConfig, mockConnectionManager, mockConsumerThreadPool);
            }

            @Override
            protected ConnectionFactory createConnectionFactory() {
                return mockConnectionFactory;
            }
            
            @Override
            protected AmqpBrokerConfig createAmqpBrokerConfig(MsbConfigurations msbConfig) {
                return amqpConfig;
            }
            
            @Override
            protected AmqpConnectionManager createConnectionManager(Connection connection) {
                return mockConnectionManager;
            }

            @Override
            protected Connection createConnection(ConnectionFactory connectionFactory) {
                return mockConnection;
            }

            @Override
            protected ExecutorService createConsumerThreadPool(AmqpBrokerConfig amqpBrokerConfig) {
                return mockConsumerThreadPool;
            }
        };
    }

    @Test
    public void testConnectionFactoryConfiguration() {
        amqpAdapterFactory.init(msbConfigurations);
        verify(mockConnectionFactory).setHost(eq(host));
        verify(mockConnectionFactory).setPort(eq(port));
        verify(mockConnectionFactory).setUsername(eq(username));
        verify(mockConnectionFactory).setPassword(eq(password));
        verify(mockConnectionFactory).setVirtualHost(eq(virtualHost));
    }

    @Test
    public void testInit() {
        amqpAdapterFactory.init(msbConfigurations);
        assertEquals(amqpAdapterFactory.getAmqpBrokerConfig(), amqpConfig);
        assertEquals(amqpAdapterFactory.getConnectionManager(), mockConnectionManager);
        assertEquals(amqpAdapterFactory.getConsumerThreadPool(), mockConsumerThreadPool);
    }

    @Test
    public void testClose() {
        amqpAdapterFactory.init(msbConfigurations);
        amqpAdapterFactory.close();
        verify(mockConsumerThreadPool).shutdown();
        try {
            verify(mockConnectionManager).close();
        } catch (IOException e) {
            fail("Can't invoke ConnectionManager.close()");
        }
    }

}
