package io.github.tcdl.msb.adapters.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.rabbitmq.client.Connection;

public class AmqpConnectionManagerTest {
    
    AmqpConnectionManager amqpConnectionManager;
    Connection mockConnection;
    
    @Before
    public void setUp() {
        mockConnection = mock(Connection.class);
        amqpConnectionManager = new AmqpConnectionManager(mockConnection);
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
