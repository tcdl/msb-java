package io.github.tcdl.adapters.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.tcdl.config.amqp.AmqpBrokerConfig;

import java.io.IOException;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class AmqpConnectionManagerTest {
    
    final String host = "127.0.0.1";
    final int port = 5672;
    final String username = "user";
    final String password = "pwd";
    final String virtualHost = "127.10.10.10"; 
    final String groupId = "msb-java";
    final boolean durable = Boolean.valueOf(false);
    final int consumerThreadPoolSize = 5;
    
    ConnectionFactory mockConnectionFactory;
    Connection mockConnection;
    AmqpBrokerConfig amqpConfig;
    AmqpConnectionManager amqpConnectionManager;
    
    @Before
    public void setUp() {
        mockConnectionFactory = mock(ConnectionFactory.class);
        mockConnection = mock(Connection.class);
        
        amqpConfig = new AmqpBrokerConfig(host, port, 
                Optional.of(username), Optional.of(password), Optional.of(virtualHost), groupId, durable, consumerThreadPoolSize);
        
        try {
            when(mockConnectionFactory.newConnection()).thenReturn(mockConnection);
        } catch (IOException e) {
            fail("Can't create MockConnectionFactory");
        }

        amqpConnectionManager = new AmqpConnectionManager(amqpConfig) {
            protected ConnectionFactory createConnectionFactory() {
                return mockConnectionFactory;
            }
        };
    }

    @Test
    public void testConnectionFactoryCreation() {
        assertEquals(amqpConnectionManager.createConnectionFactory(amqpConfig), mockConnectionFactory);
    }
    
    @Test
    public void testConnectionFactoryConfiguration() {
        verify(mockConnectionFactory).setHost(eq(host));
        verify(mockConnectionFactory).setPort(eq(port));
        verify(mockConnectionFactory).setUsername(eq(username));
        verify(mockConnectionFactory).setPassword(eq(password));
        verify(mockConnectionFactory).setVirtualHost(eq(virtualHost));
    }
    
    @Test
    public void testObtainConnection() {
        assertEquals(amqpConnectionManager.obtainConnection(), mockConnection);
    }
    
    @Test
    public void testConnectionClose() {
        when(mockConnection.isOpen()).thenReturn(true);
        try {
            amqpConnectionManager.close();
            verify(mockConnection).isOpen();
            verify(mockConnection).close();
        } catch (IOException e) {
            fail("Can't invoke ConnectionManager.close()");
        }
    }
    
    @Test
    public void testConnectionRepeatedClose() {
        when(mockConnection.isOpen()).thenReturn(false);
        try {
            amqpConnectionManager.close();
            verify(mockConnection, never()).close();
        } catch (IOException e) {
            fail("Can't invoke ConnectionManager.close()");
        }
    }

}
