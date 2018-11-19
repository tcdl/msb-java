package io.github.tcdl.msb.threading;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.ExecutorService;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MessageHandlerInvokerFactoryTest {
    @Mock
    private ConsumerExecutorFactory consumerExecutorFactory;

    private MessageHandlerInvokerFactory messageHandlerInvokerFactory;

    @Before
    public void setUp() {
        messageHandlerInvokerFactory = new MessageHandlerInvokerFactoryImpl(consumerExecutorFactory);
        when(consumerExecutorFactory.createConsumerThreadPool(anyInt(), anyInt()))
                .thenReturn(mock(ExecutorService.class));
    }

    @Test
    public void testCreateDirectHandlerInvoker() {
        MessageHandlerInvoker directInvoker = messageHandlerInvokerFactory.createDirectHandlerInvoker();

        assertThat(directInvoker, instanceOf(DirectMessageHandlerInvoker.class));
        verifyZeroInteractions(consumerExecutorFactory);
    }

    @Test
    public void testCreateExecutorBasedHandlerInvoker() {
        MessageHandlerInvoker executorBasedInvoker = messageHandlerInvokerFactory.createExecutorBasedHandlerInvoker(5, 10);

        assertThat(executorBasedInvoker, instanceOf(ExecutorBasedMessageHandlerInvoker.class));
        verify(consumerExecutorFactory).createConsumerThreadPool(5, 10);
    }

    @Test
    public void testCreateGroupedExecutorBasedHandlerInvoker() {
        MessageHandlerInvokerFactory messageHandlerInvokerFactorySpy = spy(messageHandlerInvokerFactory);
        MessageHandlerInvoker executorBasedInvoker = messageHandlerInvokerFactorySpy
                .createGroupedExecutorBasedHandlerInvoker(2, -1, mock(MessageGroupStrategy.class));

        assertThat(executorBasedInvoker, instanceOf(GroupedMessageHandlerInvoker.class));
        verify(messageHandlerInvokerFactorySpy, times(2))
                .createExecutorBasedHandlerInvoker(1, -1);
    }
}
