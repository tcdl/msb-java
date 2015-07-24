package io.github.tcdl.msb.impl;

import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.ObjectFactory;
import io.github.tcdl.msb.api.PayloadConverter;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.ResponderServer;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.api.monitor.AggregatorStats;
import io.github.tcdl.msb.monitor.aggregator.DefaultChannelMonitorAggregator;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ObjectFactoryImplTest {

    private static final String NAMESPACE = "test:object-factory";

    @Mock
    private RequestOptions requestOptionsMock;

    @Test
    public void testCreateRequester() {
        ObjectFactory objectFactory = new ObjectFactoryImpl(TestUtils.createMsbContextBuilder().build());
        Requester expectedRequester = objectFactory.createRequester(NAMESPACE, mock(RequestOptions.class), null);
        assertNotNull(expectedRequester);
    }

    @Test
    public void testCreateRequesterWithOriginalMessage() {
        ObjectFactory objectFactory = new ObjectFactoryImpl(TestUtils.createMsbContextBuilder().build());
        Requester expectedRequester = objectFactory
                .createRequester(NAMESPACE, mock(RequestOptions.class), TestUtils.createMsbRequestMessageNoPayload("test:object-factory-incoming"), Payload.class);
        assertNotNull(expectedRequester);
    }

    @Test
    public void testCreateResponderServer() {
        ObjectFactory objectFactory = new ObjectFactoryImpl(TestUtils.createMsbContextBuilder().build());
        @SuppressWarnings("unchecked")
        ResponderServer expectedResponderServer = objectFactory
                .createResponderServer(NAMESPACE, mock(MessageTemplate.class), mock(ResponderServer.RequestHandler.class));
        assertNotNull(expectedResponderServer);
    }

    @Test
    public void testShutdownNoAggregator() {
        ObjectFactoryImpl objectFactory = new ObjectFactoryImpl(TestUtils.createMsbContextBuilder().build());
        objectFactory.shutdown();
    }

    @Test
    public void testShutdownWithAggregator() {
        ObjectFactoryImpl objectFactorySpy = spy(new ObjectFactoryImpl(TestUtils.createMsbContextBuilder().build()));

        @SuppressWarnings("unchecked")
        Callback<AggregatorStats> mockAggregatorCallback = mock(Callback.class);
        DefaultChannelMonitorAggregator mockChannelMonitorAggregator = mock(DefaultChannelMonitorAggregator.class);
        when(objectFactorySpy.createDefaultChannelMonitorAggregator(Mockito.eq(mockAggregatorCallback), any(ScheduledExecutorService.class))).thenReturn(
                mockChannelMonitorAggregator);

        objectFactorySpy.createChannelMonitorAggregator(mockAggregatorCallback);
        objectFactorySpy.shutdown();

        verify(mockChannelMonitorAggregator).stop();
    }

    @Test
    public void getPayloadConverter() {
        ObjectFactory objectFactory = new ObjectFactoryImpl(TestUtils.createMsbContextBuilder().build());
        PayloadConverter payloadConverter = objectFactory.getPayloadConverter();
        assertNotNull(payloadConverter);
    }
}
