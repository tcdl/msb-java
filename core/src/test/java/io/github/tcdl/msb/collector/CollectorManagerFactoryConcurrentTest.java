package io.github.tcdl.msb.collector;

import static org.mockito.Mockito.verify;

import com.googlecode.junittoolbox.MultithreadingTester;
import io.github.tcdl.msb.ChannelManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CollectorManagerFactoryConcurrentTest {

    @Mock
    private ChannelManager channelManagerMock;

    @Test
    public void testCollectorManagerCachedMultithreadInteraction() {
        String topic = "topic:test-collector-manager-cached-multithreaded";
        CollectorManagerFactory factory  = new CollectorManagerFactory(channelManagerMock);
        new MultithreadingTester().add(() -> {
           CollectorManager collectorManager = factory.findOrCreateCollectorManager(topic);
           verify(channelManagerMock).subscribe(topic, collectorManager);
        }).run();
    }
}
