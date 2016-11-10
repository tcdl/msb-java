package io.github.tcdl.msb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.googlecode.junittoolbox.MultithreadingTester;
import io.github.tcdl.msb.adapters.AdapterFactory;
import io.github.tcdl.msb.adapters.AdapterFactoryLoader;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.ResponderOptions;
import io.github.tcdl.msb.collector.CollectorManager;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.support.JsonValidator;
import io.github.tcdl.msb.support.TestUtils;
import io.github.tcdl.msb.threading.ConsumerExecutorFactoryImpl;
import io.github.tcdl.msb.threading.MessageHandlerInvoker;
import io.github.tcdl.msb.threading.ThreadPoolMessageHandlerInvoker;
import org.junit.Before;
import org.junit.Test;

import java.time.Clock;

import static org.mockito.Mockito.*;

public class ChannelManagerConcurrentTest {

    private ChannelManager channelManager;
    private AdapterFactory adapterFactory;

    @Before
    public void setUp() {
        MsbConfig msbConfig = TestUtils.createMsbConfigurations();
        Clock clock = Clock.systemDefaultZone();
        JsonValidator validator = new JsonValidator();
        ObjectMapper messageMapper = TestUtils.createMessageMapper();
        MessageHandlerInvoker messageHandlerInvoker = new ThreadPoolMessageHandlerInvoker(msbConfig.getConsumerThreadPoolSize(), msbConfig.getConsumerThreadPoolQueueCapacity(), new ConsumerExecutorFactoryImpl());
        adapterFactory = spy(new AdapterFactoryLoader(msbConfig).getAdapterFactory());
        this.channelManager = new ChannelManager(msbConfig, clock, validator, messageMapper, adapterFactory, messageHandlerInvoker);
    }

    @Test
    public void testProducerCachedMultithreadInteraction() {
        String topic = "topic:test-producer-cached-multithreaded";

        new MultithreadingTester().add(() -> {channelManager.findOrCreateProducer(topic, RequestOptions.DEFAULTS);}).run();

        verify(adapterFactory, times(1)).createProducerAdapter(eq(topic), eq(RequestOptions.DEFAULTS));
    }

    @Test
    public void testConsumerUnsubscribeMultithreadInteraction() {
        String topic = "topic:test-remove-consumer-multithreaded";

        CollectorManager collectorManager = new CollectorManager(topic, channelManager);

        ConsumerAdapter consumerAdapter = mock(ConsumerAdapter.class);
        when(adapterFactory.createConsumerAdapter(eq(topic), any(ResponderOptions.class), eq(true))).thenReturn(consumerAdapter);

        channelManager.subscribeForResponses(topic, collectorManager);

        new MultithreadingTester().add(() -> {channelManager.unsubscribe(topic);}).run();

        verify(consumerAdapter, times(1)).unsubscribe();
    }
}
