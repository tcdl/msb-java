package io.github.tcdl;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.github.tcdl.adapters.AdapterFactory;
import io.github.tcdl.adapters.AdapterFactoryLoader;
import io.github.tcdl.adapters.ConsumerAdapter;
import io.github.tcdl.adapters.ProducerAdapter;
import io.github.tcdl.api.Callback;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.api.message.Message;
import io.github.tcdl.monitor.ChannelMonitorAgent;
import io.github.tcdl.monitor.NoopChannelMonitorAgent;
import io.github.tcdl.support.JsonValidator;
import io.github.tcdl.support.Utils;
import org.apache.commons.lang3.Validate;

/**
 * {@link ChannelManager} creates consumers or producers on demand and manages them.
 */
public class ChannelManager {

    private MsbConfigurations msbConfig;
    private Clock clock;
    private JsonValidator validator;
    private AdapterFactory adapterFactory;
    private ChannelMonitorAgent channelMonitorAgent;

    private Map<String, Producer> producersByTopic;
    private Map<String, Consumer> consumersByTopic;

    public ChannelManager(MsbConfigurations msbConfig, Clock clock, JsonValidator validator) {
        this.msbConfig = msbConfig;
        this.clock = clock;
        this.validator = validator;
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

    public void subscribe(String topic, MessageHandler messageHandler) {
        Validate.notNull(topic, "field 'topic' is null");
        Validate.notNull(messageHandler, "field 'messageHandler' is null");
        findOrCreateConsumer(topic, messageHandler);
    }

    Consumer findOrCreateConsumer(final String topic, MessageHandler messageHandler) {
        Consumer consumer = consumersByTopic.computeIfAbsent(topic, key -> {
            Consumer newConsumer = createConsumer(key, messageHandler);
            channelMonitorAgent.consumerTopicCreated(key);
            return newConsumer;
        });

        return consumer;
    }

    public synchronized void unsubscribe(String topic) {
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
        return new Producer(adapter, topic, handler);
    }

    private Consumer createConsumer(String topic, MessageHandler messageHandler) {
        Utils.validateTopic(topic);

        ConsumerAdapter adapter = getAdapterFactory().createConsumerAdapter(topic);

        return new Consumer(adapter, topic, messageHandler, msbConfig, clock, channelMonitorAgent, validator);
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
