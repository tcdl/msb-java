package io.github.tcdl.msb.impl;

import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class SimpleMessageHandlerResolverImplTest {

    @Mock
    MessageHandler messageHandler;

    Message message;

    SimpleMessageHandlerResolverImpl resolver;

    @Before
    public void setUp() {
        message = TestUtils.createSimpleResponseMessage("any");
        resolver = new SimpleMessageHandlerResolverImpl(messageHandler);
    }

    @Test
    public void testMessageHandlerResolutionByAnyMessage() {
        assertEquals(messageHandler, resolver.resolveMessageHandler(message).get());
    }

}
