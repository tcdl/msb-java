package io.github.tcdl.msb;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.adapters.AdapterFactory;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.adapters.ProducerAdapter;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.ResponderOptions;
import io.github.tcdl.msb.api.exception.ConsumerSubscriptionException;
import io.github.tcdl.msb.collector.CollectorManager;
import io.github.tcdl.msb.config.MsbConfig;
import io.github.tcdl.msb.impl.SimpleMessageHandlerResolverImpl;
import io.github.tcdl.msb.support.JsonValidator;
import io.github.tcdl.msb.support.Utils;
import io.github.tcdl.msb.threading.MessageHandlerInvoker;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link ChannelManager} creates consumers or producers on demand and manages them.
 */
public class ChannelManager {

    private static final Logger LOG = LoggerFactory.getLogger(ChannelManager.class);
    private static final String RESPONDER_LOGGING_NAME = "Responder server";

    private final MsbConfig msbConfig;
    private final Clock clock;
    private final JsonValidator validator;
    private final ObjectMapper messageMapper;
    private final AdapterFactory adapterFactory;
    private final MessageHandlerInvoker messageHandlerInvoker;

    private final Map<String, Producer> producersByTopic;
    private final Map<String, Consumer> consumersByTopic;

    public ChannelManager(MsbConfig msbConfig, Clock clock, JsonValidator validator, ObjectMapper messageMapper, AdapterFactory adapterFactory, MessageHandlerInvoker messageHandlerInvoker) {
        this.msbConfig = msbConfig;
        this.clock = clock;
        this.validator = validator;
        this.messageMapper = messageMapper;
        this.adapterFactory = adapterFactory;
        this.messageHandlerInvoker = messageHandlerInvoker;

        this.producersByTopic = new ConcurrentHashMap<>();
        this.consumersByTopic = new ConcurrentHashMap<>();
    }

    public Producer findOrCreateProducer(String topic, RequestOptions requestOptions) {
        Validate.notEmpty(topic, "Topic is mandatory");
        Validate.notNull(requestOptions, "RequestOptions are mandatory");

        Producer producer = producersByTopic.computeIfAbsent(topic, key -> {
            Producer newProducer = createProducer(key, requestOptions);
            return newProducer;
        });

        return producer;
    }

    public Optional<Long> getAvailableMessageCount(String topic) {
        return Optional.ofNullable(consumersByTopic.get(topic)).flatMap(Consumer::messageCount);
    }

    /**
     * Start consuming messages on specified topic with handler.
     * Calls to subscribe() and unsubscribe() have to be properly synchronized by client code not to lose messages.
     *
     * @param messageHandler handler for processing messages
     * @throws ConsumerSubscriptionException if subscriber for topic already exist
     */
    public synchronized void subscribe(String topic, ResponderOptions responderOptions, MessageHandler messageHandler) {
        Validate.notBlank(topic, "Topic should not be empty");
        Validate.notNull(responderOptions, "ResponderOptions are mandatory");

        MessageHandlerResolver messageHandlerResolver = new SimpleMessageHandlerResolverImpl(messageHandler, RESPONDER_LOGGING_NAME);
        subscribe(topic, false, responderOptions, messageHandlerResolver);
    }

    /**
     * {@link ChannelManager#subscribe(String, ResponderOptions, MessageHandler)} with default responderOptions
     */
    public void subscribe(String topic, MessageHandler messageHandler) {
        subscribe(topic, ResponderOptions.DEFAULTS, messageHandler);
    }

    /**
     * Start consuming response messages.
     *
     * @param topic response topic
     * @param collectorManager resolver of {@link MessageHandler}  for processing messages
     * @throws ConsumerSubscriptionException if subscriber for topic already exist
     */
    public synchronized void subscribeForResponses(String topic, CollectorManager collectorManager) {
        Validate.notBlank(topic, "Topic should not be empty");
        Validate.notNull(collectorManager, "field 'collectorManager' is null");

        subscribe(topic, true, ResponderOptions.DEFAULTS, collectorManager);
    }

    /**
     * Stop consuming messages on specified topic.
     * Calls to subscribe() and unsubscribe() have to be properly synchronized by client code not to lose messages.
     */
    public synchronized void unsubscribe(String topic) {
        if (consumersByTopic.get(topic) != null) {
            stopConsumer(consumersByTopic.remove(topic));
        }
    }

    private void subscribe(String topic, boolean isResponseTopic, ResponderOptions responderOptions, MessageHandlerResolver messageHandlerResolver) {
        if (consumersByTopic.get(topic) != null) {
            throw new ConsumerSubscriptionException("Subscriber for topic " + topic + " already exist");
        } else {
            Consumer newConsumer = createConsumer(topic, isResponseTopic, responderOptions, messageHandlerResolver);
            newConsumer.subscribe();
            consumersByTopic.put(topic, newConsumer);
        }
    }

    private void stopConsumer(Consumer consumer) {
        if (consumer != null) {
            consumer.end();
        }
    }

    private Producer createProducer(String topic, RequestOptions requestOptions) {
        Utils.validateTopic(topic);
        ProducerAdapter adapter = this.adapterFactory.createProducerAdapter(topic, requestOptions);
        return new Producer(adapter, topic, messageMapper);
    }

    private Consumer createConsumer(String topic, boolean isResponseTopic, ResponderOptions responderOptions, MessageHandlerResolver messageHandlerResolver) {
        Utils.validateTopic(topic);
        ConsumerAdapter adapter = this.adapterFactory.createConsumerAdapter(topic, responderOptions, isResponseTopic);
        return new Consumer(adapter, messageHandlerInvoker, topic, messageHandlerResolver, msbConfig, clock, validator, messageMapper);
    }

    public void shutdown() {
        LOG.info("Shutting down...");
        messageHandlerInvoker.shutdown();
        adapterFactory.shutdown();
        LOG.info("Shutdown complete");
    }
}
