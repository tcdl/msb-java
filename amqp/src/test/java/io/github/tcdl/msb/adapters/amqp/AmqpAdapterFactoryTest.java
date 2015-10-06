package io.github.tcdl.msb.adapters.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rabbitmq.client.Recoverable;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.adapters.ProducerAdapter;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.config.amqp.AmqpBrokerConfig;

import java.io.IOException;
import java.nio.charset.Charset;
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
import static org.mockito.Mockito.withSettings;

public class AmqpAdapterFactoryTest {
    final Charset charset = Charset.forName("UTF-8");
    final String host = "127.0.0.1";
    final int port = 5672;
    final String username = "user";
    final String password = "pwd";
    final String virtualHost = "127.10.10.10";
    final boolean useSSL = false;
    final String groupId = "msb-java";
    final boolean durable = false;
    final int consumerThreadPoolSize = 5;
    final int consumerThreadPoolQueueCapacity = 20;
    final boolean requeueRejectedMessages = true;
    final int heartbeatIntervalSec = 1;
    final long networkRecoveryIntervalMs = 5000;
    
    AmqpBrokerConfig amqpConfig;
    AmqpAdapterFactory amqpAdapterFactory;
    AmqpConnectionManager mockConnectionManager;
    ConnectionFactory mockConnectionFactory;
    Connection mockConnection;
    ExecutorService mockConsumerThreadPool;
    MsbConfig msbConfigurations;
    
    @Before
    public void setUp() {
        String configStr = "msbConfig {"
                + "  timerThreadPoolSize = 1\n"
                + "  brokerAdapterFactory = \"AmqpAdapterFactory\" \n"
                + "  validateMessage = true\n"
                + "  serviceDetails = {"
                + "     name = \"test_msb\" \n"
                + "     version = \"1.0.1\" \n"
                + "     instanceId = \"msbd06a-ed59-4a39-9f95-811c5fb6ab87\" \n"
                + "  } \n"
                + "}";
        Config msbConfig = ConfigFactory.parseString(configStr);
        msbConfigurations = new MsbConfig(msbConfig); 

        mockConnectionFactory = mock(ConnectionFactory.class);
        mockConnection = mock(Connection.class, withSettings().extraInterfaces(Recoverable.class));
        mockConnectionManager = mock(AmqpConnectionManager.class);
        mockConsumerThreadPool = mock(ExecutorService.class);
        
        //Define conditions for ExecutorService termination
        try {
            when(mockConsumerThreadPool.awaitTermination(anyInt(), any(TimeUnit.class))).thenReturn(true);
        } catch (InterruptedException e) {
            fail("Can't create mockConsumerThreadPool");
        }
        
        amqpConfig = new AmqpBrokerConfig(charset, host, port,
                Optional.of(username), Optional.of(password), Optional.of(virtualHost), useSSL, Optional.of(groupId), durable,
                consumerThreadPoolSize, consumerThreadPoolQueueCapacity, requeueRejectedMessages, heartbeatIntervalSec, networkRecoveryIntervalMs);
        
        amqpAdapterFactory = new AmqpAdapterFactory() {
            @Override
            public ProducerAdapter createProducerAdapter(String topic) {
                return new AmqpProducerAdapter(topic, amqpConfig, mockConnectionManager);
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
            protected AmqpBrokerConfig createAmqpBrokerConfig(MsbConfig msbConfig) {
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
    public void testInitGroupIdWithServiceName() {
        AmqpBrokerConfig amqpBrokerConfig = new AmqpAdapterFactory().createAmqpBrokerConfig(msbConfigurations);
        assertEquals(amqpBrokerConfig.getGroupId().get(), msbConfigurations.getServiceDetails().getName());
    }

    @Test
    public void testShutdown() {
        amqpAdapterFactory.init(msbConfigurations);
        amqpAdapterFactory.shutdown();
        verify(mockConsumerThreadPool).shutdown();
        try {
            verify(mockConnectionManager).close();
        } catch (IOException e) {
            fail("Can't invoke ConnectionManager.close()");
        }
    }
}
