package io.github.tcdl.msb.threading;

import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.collector.ExecutionOptionsAwareMessageHandler;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DirectInvocationCapableInvokerTest {

    DirectInvocationCapableInvoker instance;
    final String namespace = "some:namespace";

    @Mock
    AcknowledgementHandlerInternal ackHandler;

    @Test
    public void execute_shouldUseInternalDirectInvoker() throws Exception {
        MessageHandlerInvoker clientMessageHandlerInvoker = mock(MessageHandlerInvoker.class);
        instance = new DirectInvocationCapableInvoker(clientMessageHandlerInvoker);

        ExecutionOptionsAwareMessageHandler messageHandler = mock(ExecutionOptionsAwareMessageHandler.class);
        when(messageHandler.isDirectlyInvokable()).thenReturn(true);

        Message message = TestUtils.createSimpleRequestMessage(namespace);
        instance.execute(messageHandler, message, ackHandler);

        verify(messageHandler).handleMessage(eq(message), eq(ackHandler));
        verify(clientMessageHandlerInvoker, never()).execute(any(MessageHandler.class), any(Message.class), any(AcknowledgementHandlerInternal.class));
    }

    @Test
    public void execute_shouldUseClientInvoker_whenNoDirectInvocationNeeded() throws Exception {

        MessageHandlerInvoker clientMessageHandlerInvoker = mock(MessageHandlerInvoker.class);
        instance = new DirectInvocationCapableInvoker(clientMessageHandlerInvoker);

        ExecutionOptionsAwareMessageHandler messageHandler = mock(ExecutionOptionsAwareMessageHandler.class);
        when(messageHandler.isDirectlyInvokable()).thenReturn(false);

        Message message = TestUtils.createSimpleRequestMessage(namespace);
        instance.execute(messageHandler, message, ackHandler);

        verify(clientMessageHandlerInvoker).execute(eq(messageHandler), eq(message), eq(ackHandler));
    }

    @Test
    public void execute_shouldUseClientInvoker_whenHandlerIsNotDirectlyInvokable() throws Exception {
        MessageHandlerInvoker clientMessageHandlerInvoker = mock(MessageHandlerInvoker.class);
        instance = new DirectInvocationCapableInvoker(clientMessageHandlerInvoker);

        MessageHandler messageHandler = mock(MessageHandler.class);

        Message message = TestUtils.createSimpleRequestMessage(namespace);
        instance.execute(messageHandler, message, ackHandler);

        verify(clientMessageHandlerInvoker).execute(eq(messageHandler), eq(message), eq(ackHandler));
    }

    @Test
    public void execute_shouldUseClientDirectInvoker() throws Exception {
        DirectMessageHandlerInvoker clientMessageHandlerInvoker = mock(DirectMessageHandlerInvoker.class);
        instance = new DirectInvocationCapableInvoker(clientMessageHandlerInvoker);

        ExecutionOptionsAwareMessageHandler messageHandler = mock(ExecutionOptionsAwareMessageHandler.class);
        when(messageHandler.isDirectlyInvokable()).thenReturn(true);

        Message message = TestUtils.createSimpleRequestMessage(namespace);
        instance.execute(messageHandler, message, ackHandler);

        verify(clientMessageHandlerInvoker).execute(eq(messageHandler), eq(message), eq(ackHandler));
    }
}