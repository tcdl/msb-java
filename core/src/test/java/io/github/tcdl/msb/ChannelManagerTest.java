package io.github.tcdl.msb;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.adapters.AdapterFactory;
import io.github.tcdl.msb.adapters.AdapterFactoryLoader;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.ResponderOptions;
import io.github.tcdl.msb.api.exception.ConsumerSubscriptionException;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.monitor.agent.ChannelMonitorAgent;
import io.github.tcdl.msb.support.JsonValidator;
import io.github.tcdl.msb.support.TestUtils;
import io.github.tcdl.msb.threading.ConsumerExecutorFactoryImpl;
import io.github.tcdl.msb.threading.MessageHandlerInvoker;
import io.github.tcdl.msb.threading.ThreadPoolMessageHandlerInvoker;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.xml.ws.Holder;
import java.time.Clock;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ChannelManagerTest {

    private ChannelManager channelManager;
    private ChannelMonitorAgent mockChannelMonitorAgent;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() {
        MsbConfig msbConfig = TestUtils.createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();
        JsonValidator validator = new JsonValidator();
        ObjectMapper messageMapper = TestUtils.createMessageMapper();

        MessageHandlerInvoker messageHandlerInvoker = new ThreadPoolMessageHandlerInvoker(msbConfig.getConsumerThreadPoolSize(), msbConfig.getConsumerThreadPoolQueueCapacity(), new ConsumerExecutorFactoryImpl());

        AdapterFactory adapterFactory = new AdapterFactoryLoader(msbConfig).getAdapterFactory();
        this.channelManager = new ChannelManager(msbConfig, clock, validator, messageMapper, adapterFactory, messageHandlerInvoker);

        mockChannelMonitorAgent = mock(ChannelMonitorAgent.class);
        channelManager.setChannelMonitorAgent(mockChannelMonitorAgent);
    }

    @Test
    public void testProducerCached() {
        String topic = "topic:test-producer-cached";

        // Producer was created and monitor agent notified
        Producer producer1 = channelManager.findOrCreateProducer(topic, RequestOptions.DEFAULTS);
        assertNotNull(producer1);
        verify(mockChannelMonitorAgent).producerTopicCreated(topic);

        // Cached producer was returned and monitor agent wasn't notified
        Producer producer2 = channelManager.findOrCreateProducer(topic, RequestOptions.DEFAULTS);
        assertNotNull(producer2);
        assertSame(producer1, producer2);
        verifyNoMoreInteractions(mockChannelMonitorAgent);
    }

    @Test
    public void testMultipleConsumersCantSubscribeOnTheSameTopic() {
        String topic = "topic:test-consumer";

        // Consumer was created and monitor agent notified
        channelManager.subscribe(topic, (message, acknowledgeHandler) -> {});
        expectedException.expect(ConsumerSubscriptionException.class);
        channelManager.subscribe(topic, (message, acknowledgeHandler) -> {});
    }

    @Test
    public void testCantSubscribeOnTheSameTopicWithDifferentRoutingKeys() throws Exception {
        String topic = "interesting:topic";
        String bindingKey1 = "routing.key.one";
        String bindingKey2 = "routing.key.two";

        ResponderOptions responderOptions1 = new ResponderOptions.Builder().withBindingKeys(Collections.singleton(bindingKey1)).build();
        ResponderOptions responderOptions2 = new ResponderOptions.Builder().withBindingKeys(Collections.singleton(bindingKey2)).build();

        channelManager.subscribe(topic, responderOptions1, (message, acknowledgeHandler) -> {});
        verify(mockChannelMonitorAgent).consumerTopicCreated(topic);
        expectedException.expect(ConsumerSubscriptionException.class);
        channelManager.subscribe(topic, responderOptions2, (message, acknowledgeHandler) -> {});
    }

    @Test
    public void testPublishMessageInvokesAgent() {
        String topic = "topic:test-agent-publish";

        Producer producer = channelManager.findOrCreateProducer(topic, RequestOptions.DEFAULTS);
        Message message = TestUtils.createSimpleRequestMessage(topic);
        producer.publish(message);

        verify(mockChannelMonitorAgent).producerMessageSent(topic);
    }

    @Test
    public void testReceiveMessageInvokesAgentAndEmitsEvent() throws InterruptedException {
        String topic = "topic:test-agent-consume";

        CountDownLatch awaitReceiveEvents = new CountDownLatch(1);
        final Holder<Message> messageEvent = new Holder<>();

        Message message = TestUtils.createSimpleRequestMessage(topic);

        channelManager.subscribe(topic,
                (msg, acknowledgeHandler) -> {
                    messageEvent.value = msg;
                    awaitReceiveEvents.countDown();
                });
        channelManager.findOrCreateProducer(topic, RequestOptions.DEFAULTS).publish(message);

        assertTrue(awaitReceiveEvents.await(4000, TimeUnit.MILLISECONDS));
        verify(mockChannelMonitorAgent).consumerMessageReceived(topic);
        assertNotNull(messageEvent.value);
    }

    @Test
    public void testSubscribeUnsubscribeFromBroadcast() {
        String topic = "topic:test-unsubscribe-once";

        channelManager.subscribe(topic, (message, acknowledgeHandler) -> {
        });
        channelManager.unsubscribe(topic);

        verify(mockChannelMonitorAgent).consumerTopicRemoved(topic);
    }

    @Test
    public void testSubscribeUnsubscribeFromMulticast() {
        String topic = "topic:test-unsubscribe-once";

        ResponderOptions responderOptions = new ResponderOptions.Builder().withBindingKeys(Collections.singleton("routing.key")).build();

        channelManager.subscribe(topic, responderOptions, (message, acknowledgeHandler) -> {
        });
        channelManager.unsubscribe(topic);

        verify(mockChannelMonitorAgent).consumerTopicRemoved(topic);
    }

    @Test
    public void testSubscribeUnsubscribeSeparateTopics() {
        String topic1 = "topic:test-unsubscribe-try-first";
        String topic2 = "topic:test-unsubscribe-try-other";

        channelManager.subscribe(topic1, (message, acknowledgeHandler) -> {
        });
        channelManager.subscribe(topic2, (message, acknowledgeHandler) -> {
        });

        channelManager.unsubscribe(topic1);
        verify(mockChannelMonitorAgent).consumerTopicRemoved(topic1);
        verify(mockChannelMonitorAgent, never()).consumerTopicRemoved(topic2);
    }

}