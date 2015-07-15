package io.github.tcdl.msb.monitor.aggregator;

import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.ObjectFactory;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.api.monitor.ChannelMonitorAggregator;
import io.github.tcdl.msb.support.Utils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HeartbeatTaskTest {

    private ObjectFactory mockObjectFactory = mock(ObjectFactory.class);
    private Requester mockRequester = mock(Requester.class);
    @SuppressWarnings("unchecked")
    private Callback<List<Message>> mockMessageHandler = mock(Callback.class);
    private HeartbeatTask heartbeatTask = new HeartbeatTask(ChannelMonitorAggregator.DEFAULT_HEARTBEAT_TIMEOUT_MS, mockObjectFactory, mockMessageHandler);

    @Before
    public void setUp() {
        when(mockObjectFactory.createRequester(anyString(), any(RequestOptions.class))).thenReturn(mockRequester);
        @SuppressWarnings("unchecked")
        Callback<List<Message>> any = any(Callback.class);
        when(mockRequester.onEnd(any)).thenReturn(mockRequester);
    }

    @Test
    public void testRun() {
        heartbeatTask.run();

        verify(mockObjectFactory).createRequester(eq(Utils.TOPIC_HEARTBEAT), any(RequestOptions.class));
        verify(mockRequester).publish(any(Payload.class));
    }

    @Test
    public void testRunWithException() {
        try {
            when(mockObjectFactory.createRequester(anyString(), any(RequestOptions.class))).thenThrow(new RuntimeException());
            heartbeatTask.run();
        } catch (Exception e) {
            Assert.fail("Exception should not be thrown");
        }
    }

}