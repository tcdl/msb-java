package io.github.tcdl.msb.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.Producer;
import io.github.tcdl.msb.api.*;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.collector.Collector;
import io.github.tcdl.msb.events.EventHandlers;
import io.github.tcdl.msb.message.MessageFactory;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.Validate;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Implementation of {@link Requester}
 * 
 * Expected responses are matched by correlationId from original request.
 *
 * @see Requester
 */
public class RequesterImpl<T> implements Requester<T> {

    private RequestOptions requestOptions;
    private MsbContextImpl context;

    private MessageFactory messageFactory;
    private String namespace;
    EventHandlers<T> eventHandlers;
    private TypeReference<T> payloadTypeReference;

    /**
     * Creates a new instance of a requester.
     *
     * @param namespace      topic name to send a request to
     * @param requestOptions options to configure a requester
     * @param context        shared by all Requester instances
     * @return instance of a requester
     */
    static <T> RequesterImpl<T> create(String namespace, RequestOptions requestOptions, MsbContextImpl context, TypeReference<T> payloadTypeReference) {
        return new RequesterImpl<>(namespace, requestOptions, context, payloadTypeReference);
    }

    private RequesterImpl(String namespace, RequestOptions requestOptions, MsbContextImpl context, TypeReference<T> payloadTypeReference) {
        Validate.notNull(namespace, "the 'namespace' must not be null");
        Validate.notNull(requestOptions, "the 'messageOptions' must not be null");
        Validate.notNull(context, "the 'context' must not be null");

        this.namespace = namespace;
        this.requestOptions = requestOptions;
        this.context = context;
        this.payloadTypeReference = payloadTypeReference;

        this.eventHandlers = new EventHandlers<>();
        this.messageFactory = context.getMessageFactory();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(Object requestPayload) {
        publish(requestPayload, null, ArrayUtils.EMPTY_STRING_ARRAY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(Object requestPayload, String... tags) {
        publish(requestPayload, null, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(Object requestPayload, Message originalMessage) {
        publish(requestPayload, originalMessage, ArrayUtils.EMPTY_STRING_ARRAY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<T> request(Object requestPayload) {
        return request(requestPayload, null, ArrayUtils.EMPTY_STRING_ARRAY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<T> request(Object requestPayload, String... tags) {
        return request(requestPayload, null, tags);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<T> request(Object requestPayload, Message originalMessage) {
        return request(requestPayload, originalMessage, ArrayUtils.EMPTY_STRING_ARRAY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<T> request(Object requestPayload, Message originalMessage, String... tags) {
        this.eventHandlers = new EventHandlers<>(); //discard all previously set handlers

        CompletableFuture<T> futureResult = new CompletableFuture<>();

        this.onResponse((response, messageContext) -> futureResult.complete(response))
                .onAcknowledge((acknowledge, messageContext) -> {
                    boolean noResponse = !futureResult.isDone() && acknowledge.getResponsesRemaining() < 1;
                    boolean tooManyResponses = acknowledge.getResponsesRemaining() > 1;
                    if (noResponse || tooManyResponses) {
                        futureResult.cancel(true);
                    }
                })
                .onEnd(end -> {
                    if (!futureResult.isDone()) {
                        futureResult.cancel(true);
                    }
                })
                .onError((exception, message) -> futureResult.cancel(true));

        publish(true, requestOptions, requestPayload, originalMessage, tags);
        return futureResult;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(Object requestPayload, Message originalMessage, String... tags) {
        publish(false, requestOptions, requestPayload, originalMessage, tags);
    }

    private void publish(boolean invokeHandlersDirectly, RequestOptions requestOptions, Object requestPayload, Message originalMessage, String... tags) {
        MessageTemplate messageTemplate = MessageTemplate.copyOf(requestOptions.getMessageTemplate());


        if (tags != null) {
            for(String tag: tags) {
                if(tag != null) {
                    messageTemplate.addTag(tag);
                }
            }
        }
        Message.Builder messageBuilder = messageFactory.createRequestMessageBuilder(
                namespace,
                requestOptions.getForwardNamespace(),
                messageTemplate,
                originalMessage);

        Message message = messageFactory.createRequestMessage(messageBuilder, requestPayload);
        String topic = message.getTopics().getTo();

        //use Collector instance to handle expected responses/acks
        if (isWaitForAckMs() || isWaitForResponses()) {

            Collector collector = createCollector(message.getTopics().getResponse(), message, requestOptions, context, eventHandlers, invokeHandlersDirectly);
            collector.listenForResponses();

            getChannelManager().findOrCreateProducer(topic).publish(message);

            collector.waitForResponses();
        } else {
            if(requestOptions.hasRoutingKey()){
                MessageDestination destination = new MessageDestination(topic, requestOptions.getRoutingKey());
                getChannelManager().findOrCreateProducer(destination).publish(message, requestOptions.getRoutingKey());
            } else {
                getChannelManager().findOrCreateProducer(topic).publish(message);
            }
        }
    }

    private boolean isWaitForAckMs() {
        return requestOptions.getAckTimeout() != null && requestOptions.getAckTimeout() != 0;
    }

    private boolean isWaitForResponses() {
        return requestOptions.getWaitForResponses() != 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Requester<T> onAcknowledge(BiConsumer<Acknowledge, MessageContext> acknowledgeHandler) {
        eventHandlers.onAcknowledge(acknowledgeHandler);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Requester<T> onResponse(BiConsumer<T, MessageContext> responseHandler) {
        eventHandlers.onResponse(responseHandler);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override public Requester<T> onRawResponse(BiConsumer<Message, MessageContext> responseHandler) {
        eventHandlers.onRawResponse(responseHandler);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Requester<T> onEnd(Callback<Void> endHandler) {
        eventHandlers.onEnd(endHandler);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Requester<T> onError(BiConsumer<Exception, Message> errorHandler) {
        eventHandlers.onError(errorHandler);
        return this;
    }

    private ChannelManager getChannelManager() {
        return context.getChannelManager();
    }

    Collector<T> createCollector(String topic, Message requestMessage, RequestOptions requestOptions, MsbContextImpl context, EventHandlers<T> eventHandlers, boolean invokeHandlersDirectly) {
        return new Collector<>(topic, requestMessage, requestOptions, context, eventHandlers, payloadTypeReference, invokeHandlersDirectly);
    }
}
