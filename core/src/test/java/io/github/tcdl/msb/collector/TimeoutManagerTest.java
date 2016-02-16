package io.github.tcdl.msb.collector;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TimeoutManagerTest {

    @Mock
    private Collector mockCollector;

    @Before
    public void setUp() {
        when(mockCollector.getRequestMessage()).thenReturn(TestUtils.createSimpleRequestMessage("123"));
    }

    @Test
    public void testEnableResponseTimeout() {
        TimeoutManager timeoutManager = new TimeoutManager(1);
        timeoutManager.enableResponseTimeout(10, mockCollector);
        verify(mockCollector, timeout(50)).end();
    }

    @Test
    public void testEnableResponseTimeoutRejected() {
        TimeoutManager timeoutManager = new TimeoutManager(1);
        timeoutManager.shutdown();
        timeoutManager.enableResponseTimeout(10, mockCollector);
        verify(mockCollector, never()).end();
    }

    @Test
    public void testEnableResponseTimeoutMultipleTimesLastWin() {
        TimeoutManager timeoutManager = new TimeoutManager(2);
        timeoutManager.enableResponseTimeout(1000, mockCollector);
        timeoutManager.enableResponseTimeout(200, mockCollector);
        timeoutManager.enableResponseTimeout(100, mockCollector);
        timeoutManager.enableResponseTimeout(20, mockCollector);
        verify(mockCollector, timeout(50)).end();
    }

    @Test
    public void testEnableAckTimeoutPositive() {
        TimeoutManager timeoutManager = new TimeoutManager(1);
        timeoutManager.enableAckTimeout(10, mockCollector);
        verify(mockCollector, timeout(50)).end();
    }

    @Test
    public void testEnableAckTimeoutRejected() {
        TimeoutManager timeoutManager = new TimeoutManager(1);
        timeoutManager.shutdown();
        timeoutManager.enableAckTimeout(10, mockCollector);
        verify(mockCollector, never()).end();
    }

    @Test
    public void testEnableAckTimeoutNegative() {
        TimeoutManager timeoutManager = new TimeoutManager(1);
        timeoutManager.enableAckTimeout(-10, mockCollector);
        verify(mockCollector, never()).end();
    }

    @Test
    public void testEnableAckTimeoutAwaitingResponses() {
        when(mockCollector.isAwaitingResponses()).thenReturn(true);
        TimeoutManager timeoutManager = new TimeoutManager(1);
        timeoutManager.enableAckTimeout(10, mockCollector);

        verify(mockCollector, never()).end();
    }
}
