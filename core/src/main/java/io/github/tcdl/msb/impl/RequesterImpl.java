package io.github.tcdl.msb.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.api.*;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.Topics;
import io.github.tcdl.msb.collector.Collector;
import io.github.tcdl.msb.events.EventHandlers;
import io.github.tcdl.msb.message.MessageFactory;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.util.Arrays;
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

        publish(true, requestPayload, originalMessage, tags);
        return futureResult;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(Object requestPayload, Message originalMessage, String... tags) {
        publish(false, requestPayload, originalMessage, tags);
    }

    private void publish(boolean invokeHandlersDirectly, Object requestPayload, Message originalMessage, String... tags) {
        MessageTemplate messageTemplate = MessageTemplate.copyOf(requestOptions.getMessageTemplate());

        if (tags != null) {
            Arrays.stream(tags).filter(tag -> tag != null).forEach(messageTemplate::addTag);
        }

        Message.Builder messageBuilder = messageFactory.createRequestMessageBuilder(
                namespace,
                requestOptions.getForwardNamespace(),
                requestOptions.getRoutingKey(),
                messageTemplate,
                originalMessage);

        Message message = messageFactory.createRequestMessage(messageBuilder, requestPayload);

        boolean fireAndForget = !(isWaitForAckMs() || isWaitForResponses());
        boolean forwardingRequired = StringUtils.isNotBlank(requestOptions.getForwardNamespace());

        if(forwardingRequired || fireAndForget){
            publishMessage(message);
        } else {
            //set up collector for responses or acks
            Collector collector = createCollector(message, requestOptions, context, eventHandlers, invokeHandlersDirectly);
            collector.listenForResponses();

            publishMessage(message);

            collector.waitForResponses();
        }
    }

    private void publishMessage(Message message) {
        Topics topics = message.getTopics();
        if (StringUtils.isBlank(topics.getForward()) && StringUtils.isNotBlank(topics.getRoutingKey())) {
            MessageDestination destination = new MessageDestination(topics.getTo(), topics.getRoutingKey());
            getChannelManager().findOrCreateProducer(destination).publish(message);
        } else {
            getChannelManager().findOrCreateProducer(topics.getTo()).publish(message);
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

    Collector<T> createCollector(Message requestMessage,
                                 RequestOptions requestOptions,
                                 MsbContextImpl context,
                                 EventHandlers<T> eventHandlers,
                                 boolean invokeHandlersDirectly) {
        return new Collector<>(requestMessage.getTopics().getResponse(), requestMessage, requestOptions, context,
                eventHandlers, payloadTypeReference, invokeHandlersDirectly);
    }
}
