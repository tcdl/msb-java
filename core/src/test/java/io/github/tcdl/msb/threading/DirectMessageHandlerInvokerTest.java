package io.github.tcdl.msb.threading;

import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.api.message.Message;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class DirectMessageHandlerInvokerTest {

    @Mock
    MessageHandler messageHandler;

    @Mock
    AcknowledgementHandlerInternal acknowledgeHandler;

    Message message;

    @InjectMocks
    DirectMessageHandlerInvoker strategy;

    @Test
    public void testDirectInvoke() {
        strategy.execute(messageHandler, message, acknowledgeHandler);
        verify(messageHandler, times(1)).handleMessage(message, acknowledgeHandler);
        verify(acknowledgeHandler, times(1)).autoConfirm();
    }

}