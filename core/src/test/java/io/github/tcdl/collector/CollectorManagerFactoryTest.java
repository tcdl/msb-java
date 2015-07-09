package io.github.tcdl.collector;

import static org.mockito.Mockito.verify;

import io.github.tcdl.ChannelManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CollectorManagerFactoryTest {

    @Mock
    private ChannelManager channelManagerMock;

    @Test
    public void testCollectorManagerCached() {
        String topic = "topic:test-collector-manager-cached";
        CollectorManagerFactory factory  = new CollectorManagerFactory(channelManagerMock);

        CollectorManager collectorManager = factory.findOrCreateCollectorManager(topic);
        verify(channelManagerMock).subscribe(topic, collectorManager);

        collectorManager = factory.findOrCreateCollectorManager(topic);
        verify(channelManagerMock).subscribe(topic, collectorManager);
    }
}
