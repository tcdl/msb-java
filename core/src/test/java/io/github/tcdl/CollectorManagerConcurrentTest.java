package io.github.tcdl;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.googlecode.junittoolbox.MultithreadingTester;
import io.github.tcdl.support.TestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CollectorManagerConcurrentTest {

    private static final String TOPIC = "collector-subscriber-multithreaded";

    @Mock
    private ChannelManager channelManagerMock;

    @Mock
    private Collector collectorMock;

    @Before
    public void setUp() {
        when(collectorMock.getRequestMessage()).thenReturn(TestUtils.createMsbResponseMessage(TOPIC));
    }

    @Test
    public void testUnsubscribeLastCollectorMultithreadInteraction() {
        CollectorManager collectorManager = new CollectorManager(TOPIC, channelManagerMock);
        Queue<Collector> registeredCollectors = new ConcurrentLinkedQueue<>();
        for(int i = 0; i< 10; i++) {
            Collector collectorMock = mock(Collector.class);
            when(collectorMock.getRequestMessage()).thenReturn(TestUtils.createMsbResponseMessage(TOPIC));
            collectorManager.registerCollector(collectorMock);
            registeredCollectors.add(collectorMock);

        }
        Collector secondCollectorMock = mock(Collector.class);
        when(secondCollectorMock.getRequestMessage()).thenReturn(TestUtils.createMsbResponseMessage(TOPIC));

        new MultithreadingTester().numThreads(5).numRoundsPerThread(2).add(() -> {
                    collectorManager.unsubscribe(registeredCollectors.poll());
                    verify(channelManagerMock, atMost(1)).unsubscribe(TOPIC);
                }
        ).run();
    }

}
