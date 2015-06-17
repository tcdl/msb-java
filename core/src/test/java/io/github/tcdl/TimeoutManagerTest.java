package io.github.tcdl;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Created by ruslan on 03.06.15.
 */
@RunWith(MockitoJUnitRunner.class)
public class TimeoutManagerTest {

    @Mock
    Collector mockCollector;

    @Test
    public void testEnableResponseTimeout() {
        TimeoutManager timer = new TimeoutManager(1);
        timer.enableResponseTimeout(10, mockCollector);
        verify(mockCollector, timeout(50)).end();
    }

    @Test
    public void testEnableResponseTimeoutMultipleTimesLastWin() {
        TimeoutManager timer = new TimeoutManager(2);
        timer.enableResponseTimeout(1000, mockCollector);
        timer.enableResponseTimeout(200, mockCollector);
        timer.enableResponseTimeout(100, mockCollector);
        timer.enableResponseTimeout(20, mockCollector);
        verify(mockCollector, timeout(50)).end();
    }

    @Test
    public void testEnableAckTimeoutPositive() {
        TimeoutManager timer = new TimeoutManager(1);
        timer.enableAckTimeout(10, mockCollector);
        verify(mockCollector, timeout(50)).end();
    }

    @Test
    public void testEnableAckTimeoutNegative() {
        TimeoutManager timer = new TimeoutManager(1);
        timer.enableAckTimeout(-10, mockCollector);
        verify(mockCollector, never()).end();
    }

    @Test
    public void testEnableAckTimeoutAwaitingResponses() {
        when(mockCollector.isAwaitingResponses()).thenReturn(true);
        TimeoutManager timer = new TimeoutManager(1);
        timer.enableAckTimeout(10, mockCollector);

        verify(mockCollector, never()).end();
    }

}
