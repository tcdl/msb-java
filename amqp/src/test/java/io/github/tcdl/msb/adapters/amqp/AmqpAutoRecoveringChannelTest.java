package io.github.tcdl.msb.adapters.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AmqpAutoRecoveringChannelTest {

    @Mock
    AmqpConnectionManager connectionManager;
    @Mock
    Connection connection;
    @Mock
    Channel firstChannel, secondChannel;

    AmqpAutoRecoveringChannel autoRecoveringChannel;

    @Before
    public void setUp() throws Exception {
        when(connection.createChannel()).thenReturn(firstChannel);
        when(connectionManager.obtainConnection()).thenReturn(connection);

        autoRecoveringChannel = new AmqpAutoRecoveringChannel(connectionManager);
    }

    @Test
    public void testNewChannelOpened() throws Exception {
        autoRecoveringChannel.exchangeDeclare("test-exchange", "test-exchange-type", false, false, Collections.emptyMap());
        verify(connection).createChannel();
    }

    @Test
    public void testClosedChannel_replaced() throws Exception {

        Channel secondChannel = mock(Channel.class);

        when(connection.createChannel()).thenReturn(firstChannel, secondChannel);

        autoRecoveringChannel.exchangeDeclare("test-exchange", "test-exchange-type", false, false, Collections.emptyMap());
        when(firstChannel.isOpen()).thenReturn(false);
        autoRecoveringChannel.exchangeDeclare("test-exchange", "test-exchange-type", false, false, Collections.emptyMap());

        verify(connection, times(2)).createChannel();
        verify(firstChannel).abort();
        verify(firstChannel).exchangeDeclare(anyString(), anyString(), anyBoolean(), anyBoolean(), anyMapOf(String.class, Object.class));
        verify(secondChannel).exchangeDeclare(anyString(), anyString(), anyBoolean(), anyBoolean(), anyMapOf(String.class, Object.class));
    }
}