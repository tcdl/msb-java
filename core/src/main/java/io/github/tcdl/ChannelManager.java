package io.github.tcdl;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.time.DateUtils;

import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.EventEmitter;
import static io.github.tcdl.events.Event.*;
import io.github.tcdl.messages.Message;
import io.github.tcdl.support.Utils;

/**
 * Created by rdro on 4/23/2015.
 */
public class ChannelManager extends EventEmitter {

    private static ChannelManager INSTANCE = new ChannelManager();
    private MsbConfigurations msbConfig;
    private Map<String, Producer> producersByTopic;
    private Map<String, Consumer> consumersByTopic;

    public static ChannelManager getInstance() {
        return INSTANCE;
    }

    private ChannelManager() {
        this.msbConfig = MsbConfigurations.msbConfiguration();
        producersByTopic = new ConcurrentHashMap<>();
        consumersByTopic = new ConcurrentHashMap<>();
    }

    public Producer findOrCreateProducer(final String topic) {
        Validate.notNull(topic, "field 'topic' is null");
        Producer producer = producersByTopic.get(topic);
        if (producer == null) {
            producer = createProducer(topic, this.msbConfig);
            producersByTopic.put(topic, producer);

            emit(PRODUCER_NEW_TOPIC_EVENT, topic);
        }

        return producer;
    }

    public Consumer findOrCreateConsumer(final String topic, final MsbMessageOptions msgOptions) {
        Validate.notNull(topic, "field 'topic' is null");
        Consumer consumer = consumersByTopic.get(topic);
        if (consumer == null) {
            consumer = createConsumer(topic, this.msbConfig, msgOptions);
            consumersByTopic.put(topic, consumer);
            consumer.subscribe();

            emit(CONSUMER_NEW_TOPIC_EVENT, topic);
        }

        return consumer;
    }

    public void removeProducer(String topic) {
        Producer producer = producersByTopic.get(topic);
        if (producer == null)
            return;
        producersByTopic.remove(topic);
    }

    public void removeConsumer(String topic) {
        Consumer consumer = consumersByTopic.get(topic);
        if (consumer == null)
            return;

        consumer.end();
        consumersByTopic.remove(topic);

        emit(CONSUMER_REMOVED_TOPIC_EVENT, topic);
    }

    private Producer createProducer(String topic, MsbConfigurations msbConfig) {
        Utils.validateTopic(topic);
        return new Producer(topic, msbConfig).withMessageHandler(
                (message, exception) -> emit(PRODUCER_NEW_MESSAGE_EVENT, topic)
        );
    }

    private Consumer createConsumer(String topic, MsbConfigurations msbConfig, MsbMessageOptions msgOptions) {
        Utils.validateTopic(topic);
        return new Consumer(topic, msbConfig, msgOptions)
                .withMessageHandler((message, exception) -> {
                        if (isMessageExpired(message))
                            return;
                        emit(CONSUMER_NEW_MESSAGE_EVENT, topic);
                        emit(MESSAGE_EVENT, message);
                });
    }

    private boolean isMessageExpired(Message message) {
        return message.getMeta() != null
                && message.getMeta().getTtl() != null
                && DateUtils.addMilliseconds(message.getMeta().getCreatedAt(), message.getMeta().getTtl()).after(
                new Date());
    }
}
