package io.github.tcdl.msb;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.adapters.AdapterFactory;
import io.github.tcdl.msb.adapters.AdapterFactoryLoader;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.adapters.ProducerAdapter;
import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.monitor.ChannelMonitorAgent;
import io.github.tcdl.msb.monitor.NoopChannelMonitorAgent;
import io.github.tcdl.msb.support.JsonValidator;
import io.github.tcdl.msb.support.Utils;
import org.apache.commons.lang3.Validate;

/**
 * {@link ChannelManager} creates consumers or producers on demand and manages them.
 */
public class ChannelManager {

    private MsbConfig msbConfig;
    private Clock clock;
    private JsonValidator validator;
    private ObjectMapper messageMapper;
    private AdapterFactory adapterFactory;
    private ChannelMonitorAgent channelMonitorAgent;

    private Map<String, Producer> producersByTopic;
    private Map<String, Consumer> consumersByTopic;

    public ChannelManager(MsbConfig msbConfig, Clock clock, JsonValidator validator, ObjectMapper messageMapper) {
        this.msbConfig = msbConfig;
        this.clock = clock;
        this.validator = validator;
        this.messageMapper = messageMapper;
        this.adapterFactory = new AdapterFactoryLoader(msbConfig).getAdapterFactory();
        this.producersByTopic = new ConcurrentHashMap<>();
        this.consumersByTopic = new ConcurrentHashMap<>();

        channelMonitorAgent = new NoopChannelMonitorAgent();
    }

    public Producer findOrCreateProducer(final String topic) {
        Validate.notNull(topic, "field 'topic' is null");
        Producer producer = producersByTopic.computeIfAbsent(topic, key -> {
            Producer newProducer = createProducer(key);
            channelMonitorAgent.producerTopicCreated(key);
            return newProducer;
        });

        return producer;
    }

    /**
     * Start consuming messages on specified topic with handler .
     * Calls to subscribe() and unsubscribe() have to be properly synchronized by client code not to lose messages.
     *
     * @param topic
     * @param messageHandler handler for processing messages
     * @param payloadClass  define custom payload object type
     */
    public void subscribe(String topic, MessageHandler messageHandler, Class<? extends Payload> payloadClass) {
        Validate.notNull(topic, "field 'topic' is null");
        Validate.notNull(messageHandler, "field 'messageHandler' is null");
        findOrCreateConsumer(topic, messageHandler, payloadClass);
    }

    /**
     * Start consuming messages on specified topic with handler .
     * Calls to subscribe() and unsubscribe() have to be properly synchronized by client code not to lose messages.
     *
     * @param topic
     * @param messageHandler handler for processing messages
     */
    public void subscribe(String topic, MessageHandler messageHandler) {
        Validate.notNull(topic, "field 'topic' is null");
        Validate.notNull(messageHandler, "field 'messageHandler' is null");
        findOrCreateConsumer(topic, messageHandler, null);
    }

    private Consumer findOrCreateConsumer(final String topic, MessageHandler messageHandler, Class<? extends Payload> payloadClass) {
        Consumer consumer = consumersByTopic.computeIfAbsent(topic, key -> {
            Consumer newConsumer = createConsumer(key, messageHandler, payloadClass);
            channelMonitorAgent.consumerTopicCreated(key);
            return newConsumer;
        });

        return consumer;
    }

    /**
     * Stop consuming messages on specified topic.
     * Calls to subscribe() and unsubscribe() have to be properly synchronized by client code not to lose messages.     *
     * @param topic
     */
    public void unsubscribe(String topic) {
        Consumer consumer = consumersByTopic.remove(topic);
        if (consumer != null) {
            consumer.end();
            channelMonitorAgent.consumerTopicRemoved(topic);
        }
    }

    private Producer createProducer(String topic) {
        Utils.validateTopic(topic);

        ProducerAdapter adapter = getAdapterFactory().createProducerAdapter(topic);
        Callback<Message> handler = message -> channelMonitorAgent.producerMessageSent(topic);
        return new Producer(adapter, topic, handler, messageMapper);
    }

    private Consumer createConsumer(String topic, MessageHandler messageHandler, Class<? extends Payload> payloadType) {
        Utils.validateTopic(topic);

        ConsumerAdapter adapter = getAdapterFactory().createConsumerAdapter(topic);

        return new Consumer(adapter, topic, messageHandler, msbConfig, clock, channelMonitorAgent, validator, messageMapper, payloadType);
    }

    public void shutdown() {
        adapterFactory.shutdown();
    }

    public AdapterFactory getAdapterFactory() {
        return this.adapterFactory;
    }

    public void setChannelMonitorAgent(ChannelMonitorAgent channelMonitorAgent) {
        this.channelMonitorAgent = channelMonitorAgent;
    }
}
