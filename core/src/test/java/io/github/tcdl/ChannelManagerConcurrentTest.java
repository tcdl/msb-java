package io.github.tcdl;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import java.time.Clock;

import com.googlecode.junittoolbox.MultithreadingTester;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.monitor.ChannelMonitorAgent;
import io.github.tcdl.support.TestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by ruslan on 10.06.15.
 */
public class ChannelManagerConcurrentTest {

    private ChannelManager channelManager;
    private ChannelMonitorAgent mockChannelMonitorAgent;
    private Consumer.Subscriber subscriberMock;

    @Before
    public void setUp() {
        MsbConfigurations msbConfig = TestUtils.createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();
        this.channelManager = new ChannelManager(msbConfig, clock);

        mockChannelMonitorAgent = mock(ChannelMonitorAgent.class);
        channelManager.setChannelMonitorAgent(mockChannelMonitorAgent);
        subscriberMock = mock(Consumer.Subscriber.class);
    }

    @Test
    public void testProducerCachedMultithreadInteraction() {
        String topic = "topic:test-producer-cached-multithreaded";

        new MultithreadingTester().add(() -> {
            Producer producer = channelManager.findOrCreateProducer(topic);
            assertNotNull(producer);
            verify(mockChannelMonitorAgent).producerTopicCreated(topic);
        }).run();
    }

    @Test
    public void testConsumerCachedMultithreadInteraction() throws Exception {
        String topic = "topic:test-consumer-cached-multithreaded";

        new MultithreadingTester().add(() -> {
            channelManager.subscribe(topic, subscriberMock);
            verify(mockChannelMonitorAgent).consumerTopicCreated(topic);
        }).run();
    }

    @Test
    public void testRemoveConsumerMultithreadInteraction() {
        String topic = "topic:test-remove-consumer-multithreaded";

        channelManager.subscribe(topic, subscriberMock); // force creation of the consumer

        new MultithreadingTester().add(() -> {
            channelManager.unsubscribe(topic, subscriberMock);
            verify(mockChannelMonitorAgent).consumerTopicRemoved(topic);
        }).run();
    }
}
