package io.github.tcdl.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import java.time.Clock;

import io.github.tcdl.ChannelManager;
import io.github.tcdl.collector.CollectorManagerFactory;
import io.github.tcdl.collector.TimeoutManager;
import io.github.tcdl.api.MsbContext;
import io.github.tcdl.api.ObjectFactory;
import io.github.tcdl.config.MsbConfig;
import io.github.tcdl.message.MessageFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MsbContextImplTest {

    private Clock clock = Clock.systemDefaultZone();

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

    @Test
    public void testShutdown() {
        MsbContext msbContext = new MsbContextImpl(msbConfigurationsMock, messageFactoryMock, channelManagerMock, clock,
                timeoutManagerMock, collectorManagerFactoryMock);
        msbContext.shutdown();
        verify(timeoutManagerMock).shutdown();
        verify(channelManagerMock).shutdown();
    }

    @Test
    public void testSetObjectFactory() {
        ObjectFactory objectFactoryMock = mock(ObjectFactory.class);
        MsbContextImpl msbContext = new MsbContextImpl(msbConfigurationsMock, messageFactoryMock, channelManagerMock, clock,
                timeoutManagerMock, collectorManagerFactoryMock);
        msbContext.setObjectFactory(objectFactoryMock);
        assertEquals(objectFactoryMock, msbContext.getObjectFactory());
    }
}
