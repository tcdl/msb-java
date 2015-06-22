package io.github.tcdl;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import java.time.Clock;

import com.googlecode.junittoolbox.MultithreadingTester;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.EventHandlers;
import io.github.tcdl.messages.Message;
import io.github.tcdl.monitor.ChannelMonitorAgent;
import io.github.tcdl.support.JsonValidator;
import io.github.tcdl.support.TestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by ruslan on 10.06.15.
 */
public class ChannelManagerConcurrentTest {

    private ChannelManager channelManager;
    private ChannelMonitorAgent mockChannelMonitorAgent;
    private Subscriber subscriberMock;

    @Before
    public void setUp() {
        MsbConfigurations msbConfig = TestUtils.createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();
        JsonValidator validator = new JsonValidator();
        this.channelManager = new ChannelManager(msbConfig, clock, validator);

        mockChannelMonitorAgent = mock(ChannelMonitorAgent.class);
        channelManager.setChannelMonitorAgent(mockChannelMonitorAgent);
        subscriberMock = mock(Subscriber.class);
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

        Collector collector =new Collector(mock(MsbMessageOptions.class),  TestUtils.createSimpleMsbContext(), mock(EventHandlers.class));
        Message requestMessage = TestUtils.createMsbRequestMessageNoPayload();
        collector.listenForResponses(topic, requestMessage);

        channelManager.subscribe(topic, collector);

        new MultithreadingTester().add(() -> {
            channelManager.unsubscribe(topic, requestMessage.getCorrelationId());
            verify(mockChannelMonitorAgent).consumerTopicRemoved(topic);
        }).run();
    }
}
