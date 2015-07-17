package io.github.tcdl.msb.impl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import java.time.Clock;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.collector.CollectorManagerFactory;
import io.github.tcdl.msb.collector.TimeoutManager;
import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.api.ObjectFactory;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.message.MessageFactory;
import io.github.tcdl.msb.support.TestUtils;
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

    private ObjectMapper messageMapper = TestUtils.createMessageMapper();
    @Test
    public void testShutdown() {
        MsbContext msbContext = new MsbContextImpl(msbConfigurationsMock, messageFactoryMock, channelManagerMock, clock,
                timeoutManagerMock, messageMapper, collectorManagerFactoryMock);
        msbContext.shutdown();
        verify(timeoutManagerMock).shutdown();
        verify(channelManagerMock).shutdown();
    }

    @Test
    public void testSetObjectFactory() {
        ObjectFactory objectFactoryMock = mock(ObjectFactory.class);
        MsbContextImpl msbContext = new MsbContextImpl(msbConfigurationsMock, messageFactoryMock, channelManagerMock, clock,
                timeoutManagerMock, messageMapper, collectorManagerFactoryMock);
        msbContext.setObjectFactory(objectFactoryMock);
        assertEquals(objectFactoryMock, msbContext.getObjectFactory());
    }
}
