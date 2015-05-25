package io.github.tcdl;

import com.typesafe.config.Config;
import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.adapters.AdapterFactory;
import io.github.tcdl.adapters.AdapterFactoryLoader;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.events.EventEmitter;
import io.github.tcdl.events.TwoArgsEventHandler;
import io.github.tcdl.messages.Message;
import io.github.tcdl.support.Utils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.time.DateUtils;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.github.tcdl.events.Event.*;

/**
 * ChannelManager creates consumers or producers on demand
 */
public class ChannelManager extends EventEmitter {
    private AdapterFactory adapterFactory;
    private Map<String, Producer> producersByTopic;
    private Map<String, Consumer> consumersByTopic;


    public ChannelManager(Config config) {
        this(new AdapterFactoryLoader(config).getAdapterFactory());
    }

    public ChannelManager(AdapterFactory adapterFactory){
        this.adapterFactory = adapterFactory;
        this.producersByTopic = new ConcurrentHashMap<>();
        this.consumersByTopic = new ConcurrentHashMap<>();
    }


    public Producer findOrCreateProducer(final String topic) {
        Validate.notNull(topic, "field 'topic' is null");
        Producer producer = producersByTopic.get(topic);
        if (producer == null) {
            producer = createProducer(topic);
            producersByTopic.put(topic, producer);

            emit(PRODUCER_NEW_TOPIC_EVENT, topic);
        }

        return producer;
    }

    public Consumer findOrCreateConsumer(final String topic, MsbConfigurations msbConfig) {
        Validate.notNull(topic, "field 'topic' is null");
        Consumer consumer = consumersByTopic.get(topic);
        if (consumer == null) {
            consumer = createConsumer(topic, msbConfig);
            consumersByTopic.put(topic, consumer);
            consumer.subscribe();

            emit(CONSUMER_NEW_TOPIC_EVENT, topic);
        }

        return consumer;
    }

    public void removeProducer(String topic) {
        if (topic == null || !producersByTopic.containsKey(topic))
            return;
        producersByTopic.remove(topic);
    }

    public void removeConsumer(String topic) {
        if (topic == null || !consumersByTopic.containsKey(topic))
            return;
        Consumer consumer = consumersByTopic.get(topic);

        consumer.end();
        consumersByTopic.remove(topic);

        emit(CONSUMER_REMOVED_TOPIC_EVENT, topic);
    }

    private Producer createProducer(String topic) {
        Utils.validateTopic(topic);

        Adapter adapter = this.adapterFactory.createAdapter(topic);
        TwoArgsEventHandler<Message, Exception> handler = (message, exception) -> emit(PRODUCER_NEW_MESSAGE_EVENT, topic);
        return new Producer(adapter, topic, handler);
    }

    private Consumer createConsumer(String topic, MsbConfigurations msbConfig) {
        Utils.validateTopic(topic);

        Adapter adapter = this.adapterFactory.createAdapter(topic);

        TwoArgsEventHandler<Message, Exception> handler = (message, exception) -> {
            if (isMessageExpired(message))
                return;
            emit(CONSUMER_NEW_MESSAGE_EVENT, topic);
            emit(MESSAGE_EVENT, message);
        };

        return new Consumer(adapter, topic, handler, msbConfig);
    }

    private boolean isMessageExpired(Message message) {
        return message.getMeta() != null
                && message.getMeta().getTtl() != null
                && DateUtils.addMilliseconds(message.getMeta().getCreatedAt(), message.getMeta().getTtl()).after(
                        new Date());
    }
}
