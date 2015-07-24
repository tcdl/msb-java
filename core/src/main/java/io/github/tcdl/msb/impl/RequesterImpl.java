package io.github.tcdl.msb.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.collector.Collector;
import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.events.EventHandlers;
import io.github.tcdl.msb.message.MessageFactory;

import org.apache.commons.lang3.Validate;

import java.util.List;

/**
 * Internal implementations of Requester interface.
 */
public class RequesterImpl<T extends Payload> implements Requester<T> {

    private RequestOptions requestOptions;
    private MsbContextImpl context;

    private MessageFactory messageFactory;
    private Message.Builder messageBuilder;
    EventHandlers<T> eventHandlers;
    private TypeReference<T> payloadTypeReference;

    /**
     * Creates a new instance of a requester with originalMessage.
     *
     * @param namespace       topic name to send a request to
     * @param requestOptions  options to configure a requester
     * @param originalMessage original message (to take correlation id from)
     * @param context         shared by all Requester instances
     * @return instance of a requester
     */
    static <T extends  Payload> RequesterImpl<T> create(String namespace, RequestOptions requestOptions, Message originalMessage, MsbContextImpl context, TypeReference<T> payloadTypeReference) {
        return new RequesterImpl<>(namespace, requestOptions, originalMessage, context, payloadTypeReference);
    }

    private RequesterImpl(String namespace, RequestOptions requestOptions, Message originalMessage, MsbContextImpl context, TypeReference<T> payloadTypeReference) {
        Validate.notNull(namespace, "the 'namespace' must not be null");
        Validate.notNull(requestOptions, "the 'messageOptions' must not be null");
        Validate.notNull(context, "the 'context' must not be null");

        this.requestOptions = requestOptions;
        this.context = context;
        this.payloadTypeReference = payloadTypeReference;

        this.eventHandlers = new EventHandlers<>();
        this.messageFactory = context.getMessageFactory();
        this.messageBuilder = messageFactory.createRequestMessageBuilder(namespace, requestOptions.getMessageTemplate(), originalMessage);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(Payload<?, ?, ?, ?> requestPayload) {
        Message message = messageFactory.createRequestMessage(messageBuilder, requestPayload);

        //use Collector instance to handle expected responses/acks
        if (requestOptions.isWaitForResponses()) {
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
    @Override
    public Requester<T> onEnd(Callback<List<Message>> endHandler) {
        eventHandlers.onEnd(endHandler);
        return this;
    }

    private ChannelManager getChannelManager() {
        return context.getChannelManager();
    }

    Collector<T> createCollector(String topic, Message requestMessage, RequestOptions requestOptions, MsbContextImpl context, EventHandlers<T> eventHandlers) {
        return new Collector<T>(topic, requestMessage, requestOptions, context, eventHandlers, payloadTypeReference);
    }
}
