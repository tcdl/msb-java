package io.github.tcdl.msb.impl;

import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.api.message.Message;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SimpleMessageHandlerInvokeStrategyImplTest {

    @Mock
    MessageHandler messageHandler;

    @Mock
    AcknowledgementHandlerInternal acknowledgeHandler;

    Message message;

    @InjectMocks
    SimpleMessageHandlerInvokeStrategyImpl adapter;

    @Test
    public void testDirectInvoke() {
        adapter.execute(messageHandler, message, acknowledgeHandler);
        verify(messageHandler, times(1)).handleMessage(message, acknowledgeHandler);
        verify(acknowledgeHandler, times(1)).autoConfirm();
    }

}
