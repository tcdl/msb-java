package io.github.tcdl;

import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.EventHandlers;
import io.github.tcdl.messages.Acknowledge;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.Message.MessageBuilder;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.messages.payload.Payload;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Requester is a component which sends a request message to the bus and collects responses
 *
 * Created by rdro on 4/27/2015.
 */
public class Requester {

    public static final Logger LOG = LoggerFactory.getLogger(Requester.class);

    private MsbMessageOptions messageOptions;
    private MsbContext context;

    private Message message;
    private MessageFactory messageFactory;
    private MessageBuilder messageBuilder;
    EventHandlers eventHandlers;

    /**
     * Creates a new instance of a requester
     * @param messageOptions message options to construct a message
     * @param context context which contains MSB related beans
     */
    public static Requester create(MsbMessageOptions messageOptions, MsbContext context) {
        return new Requester(messageOptions, null, context);
    }

    /**
     * Creates a new instance of a requester
     * @param messageOptions message options to construct a message
     * @param originalMessage original message (to take correlation id from)
     * @param context context which contains MSB related beans
     */
    public static Requester create(MsbMessageOptions messageOptions, Message originalMessage, MsbContext context) {
        return new Requester(messageOptions, originalMessage, context);
    }

    private Requester(MsbMessageOptions messageOptions, Message originalMessage, MsbContext context) {
        Validate.notNull(messageOptions, "the 'messageOptions' must not be null");
        Validate.notNull(context, "the 'context' must not be null");

        this.messageOptions = messageOptions;
        this.context = context;

        this.eventHandlers = new EventHandlers();
        this.messageFactory = context.getMessageFactory();
        this.messageBuilder = messageFactory.createRequestMessageBuilder(messageOptions, originalMessage);
    }

    /**
     * Wraps a payload with message meta and sends to the bus
     * @param requestPayload
     */
    public void publish(@Nullable Payload requestPayload) {
        this.message = messageFactory.createRequestMessage(messageBuilder, requestPayload);

        Collector collector = createCollector(messageOptions, context, eventHandlers);

        if (collector.isWaitForResponses()) {
            String topic = message.getTopics().getResponse();
            collector.listenForResponses(topic,
                    responseMessage -> Objects.equals(responseMessage.getCorrelationId(), message.getCorrelationId())
            );
        }

        collector.getChannelManager().findOrCreateProducer(this.message.getTopics().getTo())
                .publish(this.message);

        if (collector.isWaitForResponses()) {
            collector.waitForResponses();
        } else {
            collector.end();
        }
    }

    /**
     * Registers a callback to be called when acknowledge message received
     * @param acknowledgeHandler callback
     * @return requester
     */
    public Requester onAcknowledge(Callback<Acknowledge> acknowledgeHandler) {
        eventHandlers.onAcknowledge(acknowledgeHandler);
        return this;
    }

    /**
     * Registers a callback to be called when response message received
     * @param responseHandler callback
     * @return requester
     */
    public Requester onResponse(Callback<Payload> responseHandler) {
        eventHandlers.onResponse(responseHandler);
        return this;
    }

    /**
     * Registers a callback to be called when all responses collected
     * @param endHandler callback
     * @return requester
     */
    public Requester onEnd(Callback<List<Message>> endHandler) {
        eventHandlers.onEnd(endHandler);
        return this;
    }

    /**
     * Registers a callback to be called if error happened while collecting responses
     * @param errorHandler callback
     * @return requester
     */
    public Requester onError(Callback<Exception> errorHandler) {
        eventHandlers.onError(errorHandler);
        return this;
    }

    protected Message getMessage() {
        return message;
    }

    Collector createCollector(MsbMessageOptions messageOptions, MsbContext context, EventHandlers eventHandlers) {
        return new Collector(messageOptions, context, eventHandlers);
    }
}
