package io.github.tcdl.msb;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.adapters.AdapterFactory;
import io.github.tcdl.msb.adapters.AdapterFactoryLoader;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.ResponderOptions;
import io.github.tcdl.msb.api.exception.ConsumerSubscriptionException;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.config.MsbConfig;
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
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ChannelManagerTest {

    private ChannelManager channelManager;

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
    }

    @Test
    public void testProducerCached() {
        String topic = "topic:test-producer-cached";

        // Producer was created
        Producer producer1 = channelManager.findOrCreateProducer(topic, RequestOptions.DEFAULTS);
        assertNotNull(producer1);

        // Cached producer was returned
        Producer producer2 = channelManager.findOrCreateProducer(topic, RequestOptions.DEFAULTS);
        assertNotNull(producer2);
        assertSame(producer1, producer2);
    }

    @Test
    public void testMultipleConsumersCantSubscribeOnTheSameTopic() {
        String topic = "topic:test-consumer";

        // Consumer was created
        channelManager.subscribe(topic, (message, acknowledgeHandler) -> {});
        expectedException.expect(ConsumerSubscriptionException.class);
        channelManager.subscribe(topic, (message, acknowledgeHandler) -> {});
    }

    @Test
    public void testCantSubscribeOnTheSameTopic() throws Exception {
        String topic = "interesting:topic";
        String bindingKey = "routing.key.one";

        ResponderOptions responderOptions1 = new ResponderOptions.Builder().withBindingKeys(Collections.singleton(bindingKey)).build();
        ResponderOptions responderOptions2 = new ResponderOptions.Builder().withBindingKeys(Collections.singleton(bindingKey)).build();

        channelManager.subscribe(topic, responderOptions1, (message, acknowledgeHandler) -> {});
        expectedException.expect(ConsumerSubscriptionException.class);
        channelManager.subscribe(topic, responderOptions2, (message, acknowledgeHandler) -> {});
    }

    @Test
    public void testReceiveMessageInvokesHandler() throws InterruptedException {
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
        assertNotNull(messageEvent.value);
    }

    @Test
    public void testAvailableMessageCountInitialized() {
        String topic = "some:topic";

        Optional<Long> result = channelManager.getAvailableMessageCount(topic);

        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testAvailableMessageCountSubscribed() {
        String topic = "some:topic";

        ResponderOptions responderOptions = new ResponderOptions.Builder().build();

        channelManager.subscribe(topic, responderOptions, (message, acknowledgeHandler) -> {});

        expectedException.expect(UnsupportedOperationException.class);
        channelManager.getAvailableMessageCount(topic);
    }

    @Test
    public void testAvailableMessageCountUnsubscribed() {
        String topic = "some:topic";

        ResponderOptions responderOptions = new ResponderOptions.Builder().build();

        channelManager.subscribe(topic, responderOptions, (message, acknowledgeHandler) -> {});

        expectedException.expect(UnsupportedOperationException.class);
        channelManager.getAvailableMessageCount(topic);

        channelManager.unsubscribe(topic);

        Optional<Long> result = channelManager.getAvailableMessageCount(topic);

        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testIsConsumerConnectedInitialized() {
        String topic = "some:topic";

        Optional<Boolean> result = channelManager.isConnected(topic);

        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testConsumerCountSubscribed() {
        String topic = "some:topic";

        ResponderOptions responderOptions = new ResponderOptions.Builder().build();

        channelManager.subscribe(topic, responderOptions, (message, acknowledgeHandler) -> {});

        expectedException.expect(UnsupportedOperationException.class);
        channelManager.isConnected(topic);
    }

    @Test
    public void testConsumerCountUnsubscribed() {
        String topic = "some:topic";

        ResponderOptions responderOptions = new ResponderOptions.Builder().build();

        channelManager.subscribe(topic, responderOptions, (message, acknowledgeHandler) -> {});

        expectedException.expect(UnsupportedOperationException.class);
        channelManager.isConnected(topic);

        channelManager.unsubscribe(topic);

        Optional<Long> result = channelManager.getAvailableMessageCount(topic);

        assertEquals(Optional.empty(), result);
    }
}