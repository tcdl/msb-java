package io.github.tcdl.msb;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import java.time.Clock;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.googlecode.junittoolbox.MultithreadingTester;
import io.github.tcdl.msb.api.RequesterResponderIT;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.collector.CollectorManager;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.monitor.agent.ChannelMonitorAgent;
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
        ObjectMapper messageMapper = TestUtils.createMessageMapper();
        this.channelManager = new ChannelManager(msbConfig, clock, validator, messageMapper);

        mockChannelMonitorAgent = mock(ChannelMonitorAgent.class);
        channelManager.setChannelMonitorAgent(mockChannelMonitorAgent);
        messageHandlerMock = mock(MessageHandler.class);
    }

    @Test
    public void testProducerCachedMultithreadInteraction() {
        String topic = "topic:test-producer-cached-multithreaded";

        new MultithreadingTester().add(() -> {
            channelManager.findOrCreateProducer(topic);
            verify(mockChannelMonitorAgent).producerTopicCreated(topic);
        }).run();
    }

    @Test
    public void testConsumerUnsubscribeMultithreadInteraction() {
        String topic = "topic:test-remove-consumer-multithreaded";

        CollectorManager collectorManager = new CollectorManager(topic, channelManager);
        channelManager.subscribe(topic, collectorManager);

        new MultithreadingTester().add(() -> {
            channelManager.unsubscribe(topic);
            verify(mockChannelMonitorAgent, timeout(400)).consumerTopicRemoved(topic);
        }).run();
    }

    @Test
    public void testPublishMessageInvokesAgentMultithreadInteraction() throws InterruptedException {
        String topic = "topic:test-agent-publish-multithreaded";
        int numberOfThreads = 10;
        int numberOfInvocationsPerThread = 20;

        Producer producer = channelManager.findOrCreateProducer(topic);
        Message message = TestUtils.createSimpleRequestMessage(topic);

        CountDownLatch messagesSent = new CountDownLatch(numberOfThreads * numberOfInvocationsPerThread);

        new MultithreadingTester().numThreads(numberOfThreads).numRoundsPerThread(numberOfInvocationsPerThread).add(() -> {
            producer.publish(message);
            messagesSent.countDown();
        }).run();

        assertTrue(messagesSent.await(4000, TimeUnit.MILLISECONDS));
        verify(mockChannelMonitorAgent, atLeast(numberOfThreads * numberOfInvocationsPerThread)).producerMessageSent(topic);
    }

    @Test
    public void testReceiveMessageInvokesAgentAndEmitsEventMultithreadInteraction() throws InterruptedException {
        String topic = "topic:test-agent-consumer-multithreaded";
        int numberOfThreads = 4;
        int numberOfInvocationsPerThread = 20;

        Producer producer = channelManager.findOrCreateProducer(topic);
        Message message = TestUtils.createSimpleRequestMessage(topic);

        CountDownLatch messagesReceived = new CountDownLatch(numberOfThreads * numberOfInvocationsPerThread);

        channelManager.findOrCreateProducer(topic);
        channelManager.subscribe(topic,
                msg -> {
                    messagesReceived.countDown();
                });

        new MultithreadingTester().numThreads(numberOfThreads).numRoundsPerThread(numberOfInvocationsPerThread).add(() -> {
            producer.publish(message);
        }).run();

        assertTrue(messagesReceived.await(RequesterResponderIT.MESSAGE_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
        verify(mockChannelMonitorAgent, times(numberOfThreads * numberOfInvocationsPerThread)).consumerMessageReceived(topic);
    }

}
