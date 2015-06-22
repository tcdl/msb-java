package io.github.tcdl;

import io.github.tcdl.config.RequestOptions;
import io.github.tcdl.events.EventHandlers;
import io.github.tcdl.exception.ChannelException;
import io.github.tcdl.exception.JsonConversionException;
import io.github.tcdl.messages.Acknowledge;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.Message.MessageBuilder;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.messages.payload.Payload;
import org.apache.commons.lang3.Validate;

import javax.annotation.Nullable;
import java.util.List;

/**
 * {@link Requester} is a component which sends a request message to the bus and collects responses.
 *
 * Created by rdro on 4/27/2015.
 */
public class Requester {

    private RequestOptions requestOptions;
    private MsbContext context;

    private Message message;
    private MessageFactory messageFactory;
    private MessageBuilder messageBuilder;
    EventHandlers eventHandlers;

    /**
     * Creates a new instance of a requester.
     *
     * @param namespace topic name to send a request to
     * @param requestOptions options to configure a requester
     * @param context context which contains MSB related beans
     * @return instance of a requester
     */
    public static Requester create(String namespace, RequestOptions requestOptions, MsbContext context) {
        return new Requester(namespace, requestOptions, null, context);
    }

    /**
     * Creates a new instance of a requester.
     *
     * @param namespace topic name to send a request to
     * @param requestOptions options to configure a requester
     * @param originalMessage original message (to take correlation id from)
     * @param context context which contains MSB related beans
     * @return instance of a requester
     */
    public static Requester create(String namespace, RequestOptions requestOptions, Message originalMessage, MsbContext context) {
        return new Requester(namespace, requestOptions, originalMessage, context);
    }

    private Requester(String namespace, RequestOptions requestOptions, Message originalMessage, MsbContext context) {
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
     * Wraps a payload with message meta and sends to the bus.
     *
     * @param requestPayload
     * @throws ChannelException - if an error is encountered during publishing to broker
     * @throws JsonConversionException - if unable to parse message to JSON before sending to broker
     */
    public void publish(@Nullable Payload requestPayload) {
        this.message = messageFactory.createRequestMessage(messageBuilder, requestPayload);

        //use Collector instance to handle expected responses/acks
        if (requestOptions.isWaitForResponses()) {
            Collector collector = createCollector(requestOptions, context, eventHandlers);
            String topic = message.getTopics().getResponse();
            collector.listenForResponses(topic, this.message);

            getChannelManager().findOrCreateProducer(this.message.getTopics().getTo())
                    .publish(this.message);

            collector.waitForResponses();
        } else {
            getChannelManager().findOrCreateProducer(this.message.getTopics().getTo())
                    .publish(this.message);
        }
    }

    /**
     * Registers a callback to be called when acknowledge message received.
     *
     * @param acknowledgeHandler callback
     * @return requester
     */
    public Requester onAcknowledge(Callback<Acknowledge> acknowledgeHandler) {
        eventHandlers.onAcknowledge(acknowledgeHandler);
        return this;
    }

    /**
     * Registers a callback to be called when response message received.
     *
     * @param responseHandler callback
     * @return requester
     */
    public Requester onResponse(Callback<Payload> responseHandler) {
        eventHandlers.onResponse(responseHandler);
        return this;
    }

    /**
     * Registers a callback to be called when all responses collected.
     *
     * @param endHandler callback
     * @return requester
     */
    public Requester onEnd(Callback<List<Message>> endHandler) {
        eventHandlers.onEnd(endHandler);
        return this;
    }

    protected Message getMessage() {
        return message;
    }

    private ChannelManager getChannelManager() {
        return context.getChannelManager();
    }

    Collector createCollector(RequestOptions requestOptions, MsbContext context, EventHandlers eventHandlers) {
        return new Collector(requestOptions, context, eventHandlers);
    }
}
