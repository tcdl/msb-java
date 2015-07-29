package io.github.tcdl.msb.collector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.googlecode.junittoolbox.MultithreadingTester;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.Producer;
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
        CollectorManagerFactory factory = new CollectorManagerFactory(channelManagerMock);

        CollectorManager collectorManager1 = factory.findOrCreateCollectorManager(topic);
        CollectorManager collectorManager2 = factory.findOrCreateCollectorManager(topic);

        assertEquals(collectorManager1, collectorManager2);
    }

    @Test
    public void testCollectorManagerCreatedPerTopic() {
        String topic1 = "topic:test-collector-manager-topic1";
        String topic2 = "topic:test-collector-manager-topic2";
        CollectorManagerFactory factory = new CollectorManagerFactory(channelManagerMock);

        CollectorManager collectorManager1 = factory.findOrCreateCollectorManager(topic1);
        CollectorManager collectorManager2 = factory.findOrCreateCollectorManager(topic2);

        assertNotEquals(collectorManager1, collectorManager2);
    }
}
