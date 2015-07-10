package io.github.tcdl.msb.impl;

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
 * Internal implementations of Requester interface
 */
public class RequesterImpl implements Requester {

    private RequestOptions requestOptions;
    private MsbContextImpl context;

    private MessageFactory messageFactory;
    private Message.Builder messageBuilder;
    EventHandlers eventHandlers;

    /**
     * Creates a new instance of a requester.
     *
     * @param namespace      topic name to send a request to
     * @param requestOptions options to configure a requester
     * @param context        shared by all Requester instances
     * @return instance of a requester
     */
    static RequesterImpl create(String namespace, RequestOptions requestOptions, MsbContextImpl context) {
        return new RequesterImpl(namespace, requestOptions, null, context);
    }

    /**
     * Creates a new instance of a requester with originalMessage.
     *
     * @param namespace       topic name to send a request to
     * @param requestOptions  options to configure a requester
     * @param originalMessage original message (to take correlation id from)
     * @param context         shared by all Requester instances
     * @return instance of a requester
     */
    static RequesterImpl create(String namespace, RequestOptions requestOptions, Message originalMessage, MsbContextImpl context) {
        return new RequesterImpl(namespace, requestOptions, originalMessage, context);
    }

    private RequesterImpl(String namespace, RequestOptions requestOptions, Message originalMessage, MsbContextImpl context) {
        Validate.notNull(namespace, "the 'namespace' must not be null");
        Validate.notNull(requestOptions, "the 'messageOptions' must not be null");
        Validate.notNull(context, "the 'context' must not be null");

        this.requestOptions = requestOptions;
        this.context = context;

        this.eventHandlers = new EventHandlers();
        this.messageFactory = context.getMessageFactory();
        this.messageBuilder = messageFactory.createRequestMessageBuilder(namespace, requestOptions.getMessageTemplate(), originalMessage);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(Payload requestPayload) {
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
    public Requester onAcknowledge(Callback<Acknowledge> acknowledgeHandler) {
        eventHandlers.onAcknowledge(acknowledgeHandler);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Requester onResponse(Callback<Payload> responseHandler) {
        eventHandlers.onResponse(responseHandler);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Requester onEnd(Callback<List<Message>> endHandler) {
        eventHandlers.onEnd(endHandler);
        return this;
    }

    private ChannelManager getChannelManager() {
        return context.getChannelManager();
    }

    Collector createCollector(String topic, Message requestMessage, RequestOptions requestOptions, MsbContextImpl context, EventHandlers eventHandlers) {
        return new Collector(topic, requestMessage, requestOptions, context, eventHandlers);
    }
}
