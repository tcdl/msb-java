package io.github.tcdl.msb;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import java.time.Clock;

import com.googlecode.junittoolbox.MultithreadingTester;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.monitor.ChannelMonitorAgent;
import io.github.tcdl.msb.support.JsonValidator;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Before;
import org.junit.Test;

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

}
