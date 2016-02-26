package io.github.tcdl.msb.impl;

import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.api.ObjectFactory;
import io.github.tcdl.msb.callback.MutableCallbackHandler;
import io.github.tcdl.msb.collector.CollectorManagerFactory;
import io.github.tcdl.msb.collector.TimeoutManager;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.message.MessageFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MsbContextImplTest {

    @Mock
    private MsbConfig msbConfigurationsMock;

    @Mock
    private MessageFactory messageFactoryMock;

    @Mock
    private ChannelManager channelManagerMock;

    @Mock
    private TimeoutManager timeoutManagerMock;

    @Mock
    private CollectorManagerFactory collectorManagerFactoryMock;

    @Mock
    private ObjectFactory objectFactoryMock;

    @Mock
    private MutableCallbackHandler shutdownCallbackHandlerMock;

    @Mock
    private Runnable callbackMock;

    @InjectMocks
    private MsbContextImpl msbContext;

    @Test
    public void testShutdown() {
        msbContext.setObjectFactory(objectFactoryMock);
        msbContext.shutdown();
        verifyShutdownOnce();

        msbContext.shutdown();
        msbContext.shutdown();
        verifyShutdownOnce();
    }

    private void verifyShutdownOnce() {
        verify(shutdownCallbackHandlerMock, times(1)).runCallbacks();
        verify(objectFactoryMock, times(1)).shutdown();
        verify(timeoutManagerMock, times(1)).shutdown();
        verify(channelManagerMock, times(1)).shutdown();
    }

    @Test
    public void testAddShutdownCallback() {
        msbContext.addShutdownCallback(callbackMock);
        verify(shutdownCallbackHandlerMock).add(callbackMock);
    }

    @Test
    public void testSetObjectFactory() {
        msbContext.setObjectFactory(objectFactoryMock);
        assertEquals(objectFactoryMock, msbContext.getObjectFactory());
    }
}
