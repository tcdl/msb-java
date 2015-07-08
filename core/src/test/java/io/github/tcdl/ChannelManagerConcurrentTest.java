package io.github.tcdl;

import com.googlecode.junittoolbox.MultithreadingTester;
import io.github.tcdl.config.MsbConfig;
import io.github.tcdl.monitor.ChannelMonitorAgent;
import io.github.tcdl.support.JsonValidator;
import io.github.tcdl.support.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.time.Clock;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Created by ruslan on 10.06.15.
 */
public class ChannelManagerConcurrentTest {

    private ChannelManager channelManager;
    private ChannelMonitorAgent mockChannelMonitorAgent;
    private MessageHandler messageHandlerMock;

    @Before
    public void setUp() {
        MsbConfig msbConfig = TestUtils.createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();
        JsonValidator validator = new JsonValidator();
        this.channelManager = new ChannelManager(msbConfig, clock, validator);

        mockChannelMonitorAgent = mock(ChannelMonitorAgent.class);
        channelManager.setChannelMonitorAgent(mockChannelMonitorAgent);
        messageHandlerMock = mock(MessageHandler.class);
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
            channelManager.subscribe(topic, messageHandlerMock);
            verify(mockChannelMonitorAgent).consumerTopicCreated(topic);
        }).run();
    }

    @Test
    public void testUnsubscribeMultithreadInteraction() {
        String topic = "topic:test-remove-consumer-multithreaded";

        CollectorManager collectorManager = new CollectorManager(topic, channelManager);
        channelManager.subscribe(topic, collectorManager);

        new MultithreadingTester().add(() -> {
            channelManager.unsubscribe(topic);
            verify(mockChannelMonitorAgent).consumerTopicRemoved(topic);
        }).run();
    }
}
