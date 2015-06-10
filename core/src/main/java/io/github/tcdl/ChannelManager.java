package io.github.tcdl;

import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.adapters.AdapterFactory;
import io.github.tcdl.adapters.AdapterFactoryLoader;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.messages.Message;
import io.github.tcdl.monitor.ChannelMonitorAgent;
import io.github.tcdl.monitor.NoopChannelMonitorAgent;
import io.github.tcdl.support.Utils;
import org.apache.commons.lang3.Validate;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

/**
 * ChannelManager creates consumers or producers on demand
 */
public class ChannelManager {
    
    private MsbConfigurations msbConfig;
    private Clock clock;
    private AdapterFactory adapterFactory;
    private ChannelMonitorAgent channelMonitorAgent;

    private Map<String, Producer> producersByTopic;
    private Map<String, Consumer> consumersByTopic;

    public ChannelManager(MsbConfigurations msbConfig, Clock clock) {
        this.msbConfig = msbConfig;
        this.clock = clock;
        this.adapterFactory = new AdapterFactoryLoader(msbConfig).getAdapterFactory();
        this.producersByTopic = new HashMap<>();
        this.consumersByTopic = new HashMap<>();

        channelMonitorAgent = new NoopChannelMonitorAgent();
    }

    public synchronized Producer findOrCreateProducer(final String topic) {
        Validate.notNull(topic, "field 'topic' is null");
        Producer producer = producersByTopic.get(topic);
        if (producer == null) {
            producer = createProducer(topic);
            producersByTopic.put(topic, producer);

            channelMonitorAgent.producerTopicCreated(topic);
        }

        return producer;
    }

    public synchronized void subscribe(final String topic, final Consumer.Subscriber subscriber) {
        Consumer consumer = findOrCreateConsumer(topic);
        consumer.subscribe(subscriber);
    }

    private Consumer findOrCreateConsumer(final String topic) {
        Validate.notNull(topic, "field 'topic' is null");
        Consumer consumer = consumersByTopic.get(topic);

        if (consumer == null) {
            consumer = createConsumer(topic);
            consumersByTopic.put(topic, consumer);
            channelMonitorAgent.consumerTopicCreated(topic);
        }

        return consumer;
    }

    public synchronized void unsubscribe(String topic, Consumer.Subscriber subscriber) {
        if (topic == null || !consumersByTopic.containsKey(topic))
            return;

        Consumer consumer = consumersByTopic.get(topic);
        boolean isLast = consumer.unsubscribe(subscriber);

        if (isLast) {
            consumer.end();
            consumersByTopic.remove(topic);
            channelMonitorAgent.consumerTopicRemoved(topic);
        }
    }

    private Producer createProducer(String topic) {
        Utils.validateTopic(topic);

        Adapter adapter = getAdapterFactory().createAdapter(topic);
        Callback<Message> handler = message -> channelMonitorAgent.producerMessageSent(topic);
        return new Producer(adapter, topic, handler);
    }

    private Consumer createConsumer(String topic) {
        Utils.validateTopic(topic);

        Adapter adapter = getAdapterFactory().createAdapter(topic);

        return new Consumer(adapter, topic, msbConfig, clock, channelMonitorAgent);
    }

    private AdapterFactory getAdapterFactory() {
        return this.adapterFactory;
    }

    public void setChannelMonitorAgent(ChannelMonitorAgent channelMonitorAgent) {
        this.channelMonitorAgent = channelMonitorAgent;
    }
}
