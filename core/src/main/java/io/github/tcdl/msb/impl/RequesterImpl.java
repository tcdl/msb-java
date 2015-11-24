package io.github.tcdl.msb.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.collector.Collector;
import io.github.tcdl.msb.events.EventHandlers;
import io.github.tcdl.msb.message.MessageFactory;
import org.apache.commons.lang3.Validate;

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
        publish(requestPayload, null, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(Object requestPayload, String tag) {
        publish(requestPayload, null, tag);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(Object requestPayload, Message originalMessage) {
        publish(requestPayload, originalMessage, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(Object requestPayload, Message originalMessage, String tag) {
        MessageTemplate messageTemplate = MessageTemplate.copyOf(requestOptions.getMessageTemplate());
        if (tag != null) {
            messageTemplate.addTag(tag);
        }
        Message.Builder messageBuilder = messageFactory.createRequestMessageBuilder(namespace, messageTemplate, originalMessage);
        Message message = messageFactory.createRequestMessage(messageBuilder, requestPayload);

        //use Collector instance to handle expected responses/acks
        if (isWaitForAckMs() || isWaitForResponses()) {
            String topic = message.getTopics().getResponse();

            Collector collector = createCollector(topic, message, requestOptions, context, eventHandlers);
            collector.listenForResponses();

            getChannelManager().findOrCreateProducer(message.getTopics().getTo())
                    .publish(message);

            collector.waitForResponses();
        } else {
            getChannelManager().findOrCreateProducer(message.getTopics().getTo())
                    .publish(message);
        }
    }

    private boolean isWaitForAckMs() {
        return requestOptions.getAckTimeout() != null && requestOptions.getAckTimeout() != 0;
    }

    private boolean isWaitForResponses() {
        return requestOptions.getWaitForResponses() != null && requestOptions.getWaitForResponses() != 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Requester<T> onAcknowledge(Callback<Acknowledge> acknowledgeHandler) {
        eventHandlers.onAcknowledge(acknowledgeHandler);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Requester<T> onResponse(Callback<T> responseHandler) {
        eventHandlers.onResponse(responseHandler);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override public Requester<T> onRawResponse(Callback<Message> responseHandler) {
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

    private ChannelManager getChannelManager() {
        return context.getChannelManager();
    }

    Collector<T> createCollector(String topic, Message requestMessage, RequestOptions requestOptions, MsbContextImpl context, EventHandlers<T> eventHandlers) {
        return new Collector<>(topic, requestMessage, requestOptions, context, eventHandlers, payloadTypeReference);
    }
}
