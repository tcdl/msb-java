package io.github.tcdl.msb.threading;

import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.collector.ExecutionOptionsAwareMessageHandler;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DirectInvocationCapableInvokerTest {


    final String namespace = "some:namespace";

    @Mock
    AcknowledgementHandlerInternal ackHandler;
    @Mock
    MessageHandlerInvoker clientMessageHandlerInvoker;
    @Mock
    DirectMessageHandlerInvoker directMessageHandlerInvoker;

    DirectInvocationCapableInvoker instance;

    @Before
    public void setUp() throws Exception {
        instance = new DirectInvocationCapableInvoker(clientMessageHandlerInvoker, directMessageHandlerInvoker);
    }

    @Test
    public void execute_shouldUseInternalDirectInvoker() throws Exception {
        ExecutionOptionsAwareMessageHandler messageHandler = mock(ExecutionOptionsAwareMessageHandler.class);
        when(messageHandler.forceDirectInvocation()).thenReturn(true);

        Message message = TestUtils.createSimpleRequestMessage(namespace);
        instance.execute(messageHandler, message, ackHandler);

        verify(directMessageHandlerInvoker).execute(eq(messageHandler), eq(message), eq(ackHandler));
        verify(clientMessageHandlerInvoker, never()).execute(any(MessageHandler.class), any(Message.class), any(AcknowledgementHandlerInternal.class));
    }

    @Test
    public void execute_shouldUseClientInvoker_whenNoDirectInvocationNeeded() throws Exception {
        ExecutionOptionsAwareMessageHandler messageHandler = mock(ExecutionOptionsAwareMessageHandler.class);
        when(messageHandler.forceDirectInvocation()).thenReturn(false);

        Message message = TestUtils.createSimpleRequestMessage(namespace);
        instance.execute(messageHandler, message, ackHandler);

        verify(clientMessageHandlerInvoker).execute(eq(messageHandler), eq(message), eq(ackHandler));
        verify(directMessageHandlerInvoker, never()).execute(any(MessageHandler.class), any(Message.class), any(AcknowledgementHandlerInternal.class));
    }

    @Test
    public void execute_shouldUseClientInvoker_whenHandlerIsNotDirectlyInvokable() throws Exception {
        MessageHandler messageHandler = mock(MessageHandler.class);

        Message message = TestUtils.createSimpleRequestMessage(namespace);
        instance.execute(messageHandler, message, ackHandler);

        verify(clientMessageHandlerInvoker).execute(eq(messageHandler), eq(message), eq(ackHandler));
    }

    @Test
    public void shutdown() throws Exception {
        instance.shutdown();
        verify(clientMessageHandlerInvoker).shutdown();
        verify(directMessageHandlerInvoker).shutdown();
    }
}